/**
 * Cloudflare Worker: GoHighLevel Pipeline API Proxy
 *
 * Secure backend proxy for fetching and aggregating GoHighLevel pipeline and opportunity data.
 * Includes caching, pagination handling, and pre-computed analytics endpoints.
 */

// Configuration
const GHL_BASE_URL = 'https://services.leadconnectorhq.com';
const API_VERSION = '2021-07-28';
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes in milliseconds
const GHL_PAGE_LIMIT = 100;

// In-memory cache with TTL
let cache = {
  pipelines: { data: null, timestamp: 0 },
  opportunities: { data: null, timestamp: 0 },
};

/**
 * Check if cached data is still valid
 */
function isCacheValid(cacheEntry) {
  return cacheEntry.data !== null && (Date.now() - cacheEntry.timestamp) < CACHE_TTL;
}

/**
 * Set cache entry
 */
function setCacheEntry(cacheEntry, data) {
  cacheEntry.data = data;
  cacheEntry.timestamp = Date.now();
}

/**
 * Make authenticated request to GHL API
 */
async function ghlRequest(path, env) {
  const url = `${GHL_BASE_URL}${path}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'Authorization': `Bearer ${env.GHL_API_KEY}`,
      'Version': API_VERSION,
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`GHL API error: ${response.status} ${response.statusText}`);
  }

  return await response.json();
}

/**
 * Fetch all pipelines for the location
 */
async function fetchPipelines(env) {
  if (isCacheValid(cache.pipelines)) {
    return cache.pipelines.data;
  }

  try {
    const data = await ghlRequest(`/opportunities/pipelines?locationId=${env.GHL_LOCATION_ID}`, env);
    setCacheEntry(cache.pipelines, data);
    return data;
  } catch (error) {
    console.error('Error fetching pipelines:', error);
    throw error;
  }
}

/**
 * Fetch all opportunities with pagination
 */
async function fetchAllOpportunities(env) {
  if (isCacheValid(cache.opportunities)) {
    return cache.opportunities.data;
  }

  try {
    const allOpportunities = [];
    let nextPageUrl = null;
    let pageCount = 0;

    // Initial request
    let response = await ghlRequest(
      `/opportunities/search?location_id=${env.GHL_LOCATION_ID}&limit=${GHL_PAGE_LIMIT}`,
      env
    );

    allOpportunities.push(...(response.opportunities || []));
    nextPageUrl = response.meta?.nextPageUrl;
    pageCount++;

    // Paginate through remaining results
    while (nextPageUrl && pageCount < 100) { // Safety limit: max 100 pages (10,000 records)
      const pageUrlObj = new URL(nextPageUrl, GHL_BASE_URL);
      const pathname = pageUrlObj.pathname + pageUrlObj.search;

      response = await ghlRequest(pathname, env);
      allOpportunities.push(...(response.opportunities || []));
      nextPageUrl = response.meta?.nextPageUrl;
      pageCount++;
    }

    const result = {
      opportunities: allOpportunities,
      totalCount: allOpportunities.length,
      pagesFetched: pageCount,
    };

    setCacheEntry(cache.opportunities, result);
    return result;
  } catch (error) {
    console.error('Error fetching opportunities:', error);
    throw error;
  }
}

/**
 * Compute pipeline statistics
 */
async function computePipelineStats(env) {
  try {
    const [pipelinesData, opportunitiesData] = await Promise.all([
      fetchPipelines(env),
      fetchAllOpportunities(env),
    ]);

    const pipelines = pipelinesData.pipelines || [];
    const opportunities = opportunitiesData.opportunities || [];

    // Build pipeline and stage lookup maps
    const pipelineMap = new Map();
    const stageMap = new Map(); // Map of stageId -> { pipelineId, name }

    pipelines.forEach(pipeline => {
      pipelineMap.set(pipeline.id, {
        id: pipeline.id,
        name: pipeline.name,
      });

      (pipeline.stages || []).forEach(stage => {
        stageMap.set(stage.id, {
          pipelineId: pipeline.id,
          pipelineName: pipeline.name,
          stageId: stage.id,
          stageName: stage.name,
        });
      });
    });

    // Initialize stats structure
    let unmatchedCount = 0;
    const unmatchedStageIds = new Set();
    const stats = {
      pipelines: {},
      summary: {
        totalOpportunities: opportunities.length,
        totalValue: 0,
        averageValue: 0,
      },
      generatedAt: new Date().toISOString(),
    };

    // Aggregate opportunities by pipeline and stage
    opportunities.forEach(opp => {
      const stageInfo = stageMap.get(opp.pipelineStageId) || stageMap.get(opp.stageId);

      if (!stageInfo) {
        unmatchedCount++;
        unmatchedStageIds.add(opp.pipelineStageId || opp.stageId || 'none');
        return;
      }

      const pipelineId = stageInfo.pipelineId;

      // Initialize pipeline if needed
      if (!stats.pipelines[pipelineId]) {
        stats.pipelines[pipelineId] = {
          id: pipelineId,
          name: stageInfo.pipelineName,
          totalOpportunities: 0,
          totalValue: 0,
          stages: {},
        };
      }

      // Initialize stage if needed — use stageInfo.stageId (the matched ID from pipeline definition)
      const resolvedStageId = stageInfo.stageId;
      if (!stats.pipelines[pipelineId].stages[resolvedStageId]) {
        stats.pipelines[pipelineId].stages[resolvedStageId] = {
          id: resolvedStageId,
          name: stageInfo.stageName,
          count: 0,
          totalValue: 0,
          averageValue: 0,
          averageDaysInStage: 0,
          opportunities: [],
        };
      }

      const stage = stats.pipelines[pipelineId].stages[resolvedStageId];
      const value = parseFloat(opp.monetaryValue) || 0;
      const createdAt = new Date(opp.createdAt);
      const daysInStage = Math.floor((Date.now() - createdAt.getTime()) / (1000 * 60 * 60 * 24));

      stage.count++;
      stage.totalValue += value;
      stage.opportunities.push({
        id: opp.id,
        name: opp.name,
        value: value,
        daysInStage: daysInStage,
        createdAt: opp.createdAt,
      });

      stats.pipelines[pipelineId].totalOpportunities++;
      stats.pipelines[pipelineId].totalValue += value;
      stats.summary.totalValue += value;
    });

    // Calculate averages
    Object.values(stats.pipelines).forEach(pipeline => {
      Object.values(pipeline.stages).forEach(stage => {
        stage.averageValue = stage.count > 0 ? stage.totalValue / stage.count : 0;
        const daysInStageSum = stage.opportunities.reduce((sum, opp) => sum + opp.daysInStage, 0);
        stage.averageDaysInStage = stage.count > 0 ? daysInStageSum / stage.count : 0;
      });
    });

    stats.summary.averageValue = opportunities.length > 0
      ? stats.summary.totalValue / opportunities.length
      : 0;
    stats.debug = {
      pipelineCount: pipelines.length,
      stageCount: stageMap.size,
      opportunityCount: opportunities.length,
      unmatchedCount,
      unmatchedStageIds: [...unmatchedStageIds].slice(0, 10),
      sampleOppKeys: opportunities.length > 0 ? Object.keys(opportunities[0]) : [],
      sampleOpp: opportunities.length > 0 ? {
        id: opportunities[0].id,
        stageId: opportunities[0].stageId,
        pipelineStageId: opportunities[0].pipelineStageId,
        pipelineId: opportunities[0].pipelineId,
        status: opportunities[0].status,
      } : null,
      stageIds: [...stageMap.keys()].slice(0, 10),
    };

    return stats;
  } catch (error) {
    console.error('Error computing pipeline stats:', error);
    throw error;
  }
}

/**
 * Compute lead source analytics (Google vs Facebook vs other)
 *
 * Lead source is determined by GHL TAGS on the contact:
 *   - "facebook lead" tag  →  Facebook bucket
 *   - "sm3" tag            →  Google bucket
 *   - Neither              →  Other bucket
 *
 * Contract detection is NOT done here — the frontend cross-references
 * contact names against the Deal Ledger (source of truth for contracts).
 * This endpoint returns every lead's name and source bucket so the
 * frontend can do the matching.
 *
 * Supports optional ?months=N query param to limit to last N months.
 */
async function computeLeadSourceStats(env, monthsBack) {
  try {
    const opportunitiesData = await fetchAllOpportunities(env);
    const opportunities = opportunitiesData.opportunities || [];

    // Date filter: only include leads from the last N months
    const cutoffDate = monthsBack
      ? new Date(Date.now() - monthsBack * 30.44 * 24 * 60 * 60 * 1000)
      : null;

    const sourceBuckets = {
      google: { leads: [], byMonth: {} },
      facebook: { leads: [], byMonth: {} },
      other: { leads: [], byMonth: {} },
      combined: { leads: [], byMonth: {} },
    };

    const allTags = {};

    opportunities.forEach(opp => {
      const createdAt = new Date(opp.createdAt || opp.dateAdded || Date.now());

      // Apply date filter
      if (cutoffDate && createdAt < cutoffDate) return;

      const monthKey = `${createdAt.getFullYear()}-${String(createdAt.getMonth() + 1).padStart(2, '0')}`;

      // Extract tags from the CONTACT object nested inside the opportunity
      const contactTags = (opp.contact?.tags || []).map(t => (typeof t === 'string' ? t : (t.name || '')).toLowerCase().trim());
      const oppTags = (opp.tags || []).map(t => (typeof t === 'string' ? t : (t.name || '')).toLowerCase().trim());
      const allTagsForOpp = [...contactTags, ...oppTags];

      allTagsForOpp.forEach(t => { if (t) allTags[t] = (allTags[t] || 0) + 1; });

      // Also check opportunity source field as fallback
      const oppSource = (opp.source || '').toLowerCase();

      // Bucket by tag first, then by source field
      let bucket = 'other';
      if (allTagsForOpp.some(t => t === 'facebook lead' || t.includes('facebook lead'))) {
        bucket = 'facebook';
      } else if (allTagsForOpp.some(t => t === 'sm3' || t.includes('sm3'))) {
        bucket = 'google';
      } else if (oppSource.includes('ppc') || oppSource.includes('google')) {
        bucket = 'google';
      } else if (oppSource.includes('facebook') || oppSource.includes('fb') || oppSource.includes('meta')) {
        bucket = 'facebook';
      }

      // Build lead record with contact name for frontend cross-referencing
      const contactName = (opp.contact?.name || opp.name || '').trim();
      const contactEmail = (opp.contact?.email || '').trim();
      const contactPhone = (opp.contact?.phone || '').trim();

      const leadRecord = {
        name: contactName,
        email: contactEmail,
        phone: contactPhone,
        createdAt: opp.createdAt,
        month: monthKey,
        source: bucket,
        oppSource: opp.source || null,
        tags: allTagsForOpp.filter(t => t),
      };

      sourceBuckets[bucket].leads.push(leadRecord);
      sourceBuckets.combined.leads.push(leadRecord);

      // Monthly lead counts
      if (!sourceBuckets[bucket].byMonth[monthKey]) {
        sourceBuckets[bucket].byMonth[monthKey] = { leads: 0 };
      }
      if (!sourceBuckets.combined.byMonth[monthKey]) {
        sourceBuckets.combined.byMonth[monthKey] = { leads: 0 };
      }
      sourceBuckets[bucket].byMonth[monthKey].leads++;
      sourceBuckets.combined.byMonth[monthKey].leads++;
    });

    // Build result — include lead names for cross-referencing but keep payload reasonable
    const result = {};
    for (const [key, data] of Object.entries(sourceBuckets)) {
      result[key] = {
        leadCount: data.leads.length,
        byMonth: data.byMonth,
        // Include all lead names/emails for cross-referencing against Deal Ledger
        leadNames: data.leads.map(l => l.name).filter(n => n),
        leadDetails: key !== 'other' ? data.leads : [], // Full details for google/facebook only (other is too large)
      };
    }

    return {
      sources: result,
      totalOpportunities: opportunities.length,
      filteredCount: sourceBuckets.combined.leads.length,
      monthsBack: monthsBack || 'all',
      uniqueTags: allTags,
      generatedAt: new Date().toISOString(),
    };
  } catch (error) {
    console.error('Error computing lead source stats:', error);
    throw error;
  }
}

// ==================== ZILLOW API (RapidAPI) ====================

const ZILLOW_API_HOST = 'zllw-working-api.p.rapidapi.com';

/**
 * Zillow API cache (separate from GHL cache)
 */
let zillowCache = {};
const ZILLOW_CACHE_TTL = 30 * 60 * 1000; // 30 minutes

function getZillowCache(key) {
  const entry = zillowCache[key];
  if (entry && (Date.now() - entry.timestamp) < ZILLOW_CACHE_TTL) {
    return entry.data;
  }
  return null;
}

function setZillowCache(key, data) {
  zillowCache[key] = { data, timestamp: Date.now() };
  // Limit cache size
  const keys = Object.keys(zillowCache);
  if (keys.length > 100) {
    const oldest = keys.sort((a, b) => zillowCache[a].timestamp - zillowCache[b].timestamp);
    oldest.slice(0, 20).forEach(k => delete zillowCache[k]);
  }
}

/**
 * Make authenticated request to Zillow RapidAPI
 */
async function zillowRequest(path, env) {
  if (!env.ZILLOW_API_KEY) {
    throw new Error('ZILLOW_API_KEY not configured');
  }

  const url = `https://${ZILLOW_API_HOST}${path}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      'x-rapidapi-key': env.ZILLOW_API_KEY,
      'x-rapidapi-host': ZILLOW_API_HOST,
    },
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Zillow API error: ${response.status} ${response.statusText} - ${text.substring(0, 200)}`);
  }

  return await response.json();
}

/**
 * Get property info by address (ZLLW Working API - Property Info Advanced)
 * Endpoint: /byaddress
 */
async function zillowGetByAddress(address, env) {
  const cacheKey = 'addr_' + address;
  const cached = getZillowCache(cacheKey);
  if (cached) return cached;

  const encoded = encodeURIComponent(address);
  const data = await zillowRequest(`/pro/byaddress?propertyaddress=${encoded}`, env);
  setZillowCache(cacheKey, data);
  return data;
}

/**
 * Get detailed property info by zpid (ZLLW Working API)
 * Endpoint: /byzpid
 */
async function zillowGetByZpid(zpid, env) {
  const cacheKey = 'zpid_' + zpid;
  const cached = getZillowCache(cacheKey);
  if (cached) return cached;

  const data = await zillowRequest(`/pro/byzpid?zpid=${zpid}`, env);
  setZillowCache(cacheKey, data);
  return data;
}

/**
 * Search for properties by location (zip, city, coordinates) using ZLLW search endpoint.
 * Tries multiple endpoint patterns since ZLLW API naming varies.
 * Returns an array of property stubs (zpid, address, price, status, etc.)
 */
async function zillowSearchByLocation(location, status_type, env) {
  const cacheKey = 'search_' + location + '_' + (status_type || 'recentlySold');
  const cached = getZillowCache(cacheKey);
  if (cached) return cached;

  // Try multiple search endpoint patterns that ZLLW API might support
  const searchEndpoints = [
    `/pro/search?location=${encodeURIComponent(location)}&status_type=${status_type || 'RecentlySold'}&home_type=Houses`,
    `/propertyExtendedSearch?location=${encodeURIComponent(location)}&status_type=${status_type || 'RecentlySold'}&home_type=Houses`,
    `/search?location=${encodeURIComponent(location)}&status=${status_type || 'recentlySold'}`,
  ];

  for (const endpoint of searchEndpoints) {
    try {
      const data = await zillowRequest(endpoint, env);
      if (data && (data.props || data.results || data.searchResults || data.properties || data.zpids || Array.isArray(data))) {
        setZillowCache(cacheKey, data);
        return data;
      }
      // Check if response has meaningful nested property data
      if (data && typeof data === 'object') {
        const extracted = deepExtractProperties(data, 0, 4, null);
        if (extracted.length >= 3) {
          setZillowCache(cacheKey, data);
          return data;
        }
      }
    } catch (e) {
      // Endpoint doesn't exist or failed — try next one
      continue;
    }
  }

  return null; // No search endpoint worked
}

// ==================== RENTOMETER API ====================

const RENTOMETER_BASE = 'https://www.rentometer.com/api/v1';

/**
 * Rentometer Rent Summary (QuickView) — returns median, mean, range, samples
 * Endpoint: GET /api/v1/summary
 */
async function rentometerSummary(address, bedrooms, env) {
  if (!env.RENTOMETER_API_KEY) {
    throw new Error('RENTOMETER_API_KEY not configured');
  }

  const cacheKey = 'rento_' + address + '_' + bedrooms;
  const cached = getZillowCache(cacheKey); // reuse same cache system
  if (cached) return cached;

  const params = new URLSearchParams({
    api_key: env.RENTOMETER_API_KEY,
    address: address,
    bedrooms: String(bedrooms || 3),
    building_type: 'house',
  });

  const response = await fetch(`${RENTOMETER_BASE}/summary?${params.toString()}`);

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Rentometer API error: ${response.status} - ${text.substring(0, 200)}`);
  }

  const data = await response.json();
  setZillowCache(cacheKey, data);
  return data;
}

/**
 * Extract listing agents from Zillow property data.
 * Recursively scans for agent/broker info (attributionInfo, listingAgent, brokerageName, etc.)
 * Used to find active investor-friendly listing agents near a subject property.
 */
// Regex to detect phone numbers in values (US format) — shared across agent functions
const phoneRegex = /(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/;

function deepExtractAgents(obj, depth, maxDepth) {
  if (!obj || depth > maxDepth) return [];
  const results = [];

  // Brute-force scan an object for phone-like string values
  function findPhonesInObj(obj, maxDepth = 3, depth = 0) {
    const phones = [];
    if (!obj || depth > maxDepth) return phones;
    if (typeof obj === 'string') {
      const m = obj.match(phoneRegex);
      if (m) phones.push(m[0]);
      return phones;
    }
    if (typeof obj === 'number' && String(obj).length >= 10) {
      phones.push(String(obj));
      return phones;
    }
    if (Array.isArray(obj)) {
      obj.forEach(v => phones.push(...findPhonesInObj(v, maxDepth, depth + 1)));
    } else if (typeof obj === 'object') {
      for (const [key, val] of Object.entries(obj)) {
        const k = key.toLowerCase();
        // Prioritize keys that look phone-related
        if (k.includes('phone') || k.includes('cell') || k.includes('mobile') || k.includes('tel') || k.includes('contact')) {
          if (typeof val === 'string' && val.match(phoneRegex)) phones.unshift(val.match(phoneRegex)[0]);
          else if (typeof val === 'number' && String(val).length >= 10) phones.unshift(String(val));
        } else {
          phones.push(...findPhonesInObj(val, maxDepth, depth + 1));
        }
      }
    }
    return phones;
  }

  // Find any phone from many possible sources
  function findPhone(...sources) {
    for (const s of sources) {
      if (s && typeof s === 'string' && s.length >= 7) return s;
      if (s && typeof s === 'number' && String(s).length >= 10) return String(s);
    }
    return null;
  }

  // Find agent name from an object (checks many field names)
  function findAgentName(obj) {
    if (!obj || typeof obj !== 'object') return null;
    return obj.agentName || obj.name || obj.agent_name || obj.display_name
      || obj.listAgentName || obj.listingAgentName || obj.buyerAgentName
      || obj.sellerAgentName || obj.coAgentName || null;
  }

  function extractAgentFromItem(item) {
    const agents = [];

    // Check attributionInfo
    const attrInfo = item.attributionInfo || {};
    if (attrInfo.agentName || attrInfo.brokerName) {
      const phonesInAttr = findPhonesInObj(attrInfo);
      agents.push({
        agentName: attrInfo.agentName || null,
        agentPhone: findPhone(attrInfo.agentPhoneNumber, attrInfo.agentPhone, attrInfo.phone, attrInfo.listAgentPhone) || phonesInAttr[0] || null,
        brokerName: attrInfo.brokerName || null,
        brokerPhone: findPhone(attrInfo.brokerPhoneNumber, attrInfo.brokerPhone) || phonesInAttr[1] || null,
        mlsId: attrInfo.mlsId || null,
      });
    }

    // Check listingAgent object
    if (item.listingAgent && (item.listingAgent.name || item.listingAgent.agentName)) {
      const la = item.listingAgent;
      const phonesInLA = findPhonesInObj(la);
      agents.push({
        agentName: la.name || la.agentName || null,
        agentPhone: findPhone(la.phone, la.phoneNumber, la.agentPhone, la.cellPhone, la.officePhone, la.directPhone) || phonesInLA[0] || null,
        brokerName: la.brokerageName || la.officeName || item.brokerageName || null,
        brokerPhone: findPhone(la.officePhoneNumber, la.brokerPhone) || phonesInLA[1] || null,
        mlsId: la.mlsId || null,
      });
    }

    // Check role-based agents
    ['buyerAgent', 'sellerAgent', 'coAgent', 'listAgent', 'listing_agent', 'seller_agent'].forEach(role => {
      const ag = item[role];
      if (ag && typeof ag === 'object' && findAgentName(ag)) {
        const phonesInAg = findPhonesInObj(ag);
        agents.push({
          agentName: findAgentName(ag),
          agentPhone: findPhone(ag.phone, ag.phoneNumber, ag.cellPhone, ag.directPhone, ag.officePhone) || phonesInAg[0] || null,
          brokerName: ag.brokerageName || ag.officeName || null,
          brokerPhone: null,
          mlsId: ag.mlsId || null,
        });
      }
    });

    // Check contactRecipients (Zillow often puts agent contact info here)
    if (Array.isArray(item.contactRecipients)) {
      item.contactRecipients.forEach(cr => {
        if (cr && (cr.agentName || cr.name || cr.agent_name)) {
          const phonesInCR = findPhonesInObj(cr);
          agents.push({
            agentName: cr.agentName || cr.name || cr.agent_name || null,
            agentPhone: findPhone(cr.phone, cr.phoneNumber, cr.agentPhone) || phonesInCR[0] || null,
            brokerName: cr.brokerageName || cr.badgeType || null,
            brokerPhone: null,
            mlsId: null,
          });
        }
      });
    }

    // Check listingProvider
    if (item.listingProvider && (item.listingProvider.agentName || item.listingProvider.name)) {
      const lp = item.listingProvider;
      const phonesInLP = findPhonesInObj(lp);
      agents.push({
        agentName: lp.agentName || lp.name || null,
        agentPhone: findPhone(lp.phone, lp.phoneNumber) || phonesInLP[0] || null,
        brokerName: lp.title || lp.brokerageName || null,
        brokerPhone: null,
        mlsId: null,
      });
    }

    // Flat field fallback
    if (item.agentName || item.listingAgentName || item.listAgentName) {
      const phonesFlat = findPhonesInObj(item, 1); // shallow scan
      agents.push({
        agentName: item.agentName || item.listingAgentName || item.listAgentName || null,
        agentPhone: findPhone(item.agentPhone, item.listingAgentPhone, item.listAgentPhone, item.agentPhoneNumber, item.phone, item.contactPhone) || phonesFlat[0] || null,
        brokerName: item.brokerageName || item.brokerName || item.officeName || null,
        brokerPhone: findPhone(item.brokerPhone, item.officePhone) || null,
        mlsId: null,
      });
    }

    return agents;
  }

  if (Array.isArray(obj)) {
    obj.forEach(item => {
      if (item && typeof item === 'object') {
        const agents = extractAgentFromItem(item);
        agents.forEach(a => {
          if (a.agentName) {
            // Attach property context
            a.propertyAddress = item.address || item.streetAddress || item.formattedAddress || '';
            if (typeof a.propertyAddress === 'object') a.propertyAddress = (a.propertyAddress.streetAddress || '') + ', ' + (a.propertyAddress.city || '');
            a.listPrice = item.price || item.listPrice || null;
            a.homeStatus = item.homeStatus || item.statusType || item.listingStatus || '';
            a.zpid = item.zpid || null;
            a.bedrooms = item.bedrooms || item.beds || null;
            a.bathrooms = item.bathrooms || item.baths || null;
            a.livingArea = item.livingArea || item.sqft || null;
            results.push(a);
          }
        });
        results.push(...deepExtractAgents(item, depth + 1, maxDepth));
      }
    });
  } else if (typeof obj === 'object') {
    const agents = extractAgentFromItem(obj);
    agents.forEach(a => {
      if (a.agentName) {
        a.propertyAddress = obj.address || obj.streetAddress || '';
        if (typeof a.propertyAddress === 'object') a.propertyAddress = (a.propertyAddress.streetAddress || '') + ', ' + (a.propertyAddress.city || '');
        a.listPrice = obj.price || obj.listPrice || null;
        a.homeStatus = obj.homeStatus || obj.statusType || '';
        a.zpid = obj.zpid || null;
        a.bedrooms = obj.bedrooms || obj.beds || null;
        a.bathrooms = obj.bathrooms || obj.baths || null;
        a.livingArea = obj.livingArea || obj.sqft || null;
        results.push(a);
      }
    });
    Object.values(obj).forEach(val => {
      if (val && typeof val === 'object') {
        results.push(...deepExtractAgents(val, depth + 1, maxDepth));
      }
    });
  }
  return results;
}

/**
 * Find active listing agents near a property — focused on investor-price-range listings.
 * Fetches property data, extracts all agent info, deduplicates by agent name,
 * and returns agents sorted by number of listings (most active first).
 */
async function findListingAgents(address, env) {
  const propertyData = await zillowGetByAddress(address, env);
  const p = propertyData?.propertyDetails || propertyData?.data || propertyData || {};

  // Extract all agents from the full response tree
  const rawAgents = deepExtractAgents(propertyData, 0, 6);

  // Deduplicate by agent name, track their listings
  const agentMap = {};
  rawAgents.forEach(a => {
    if (!a.agentName) return;
    const key = a.agentName.toLowerCase().trim();
    if (!agentMap[key]) {
      agentMap[key] = {
        agentName: a.agentName,
        agentPhone: a.agentPhone,
        brokerName: a.brokerName,
        brokerPhone: a.brokerPhone,
        listings: [],
      };
    }
    // Update phone if we didn't have it
    if (!agentMap[key].agentPhone && a.agentPhone) agentMap[key].agentPhone = a.agentPhone;
    if (!agentMap[key].brokerName && a.brokerName) agentMap[key].brokerName = a.brokerName;
    // Add listing
    if (a.propertyAddress && a.listPrice) {
      const alreadyHas = agentMap[key].listings.some(l => l.zpid === a.zpid && a.zpid);
      if (!alreadyHas) {
        agentMap[key].listings.push({
          address: a.propertyAddress,
          price: a.listPrice,
          status: a.homeStatus,
          zpid: a.zpid,
          beds: a.bedrooms,
          baths: a.bathrooms,
          sqft: a.livingArea,
        });
      }
    }
  });

  // Convert to array, sort by listing count (most active first)
  let agents = Object.values(agentMap);
  agents.sort((a, b) => b.listings.length - a.listings.length);

  // Focus on lower-price-range agents (investor-friendly)
  // Calculate median price of all listings to establish "investor range"
  const allPrices = agents.flatMap(a => a.listings.map(l => l.price)).filter(p => p > 0).sort((a, b) => a - b);
  const medianPrice = allPrices.length > 0 ? allPrices[Math.floor(allPrices.length / 2)] : 0;
  // Tag each agent with their avg price and whether they list investor-range properties
  agents.forEach(a => {
    const prices = a.listings.map(l => l.price).filter(p => p > 0);
    a.avgListPrice = prices.length > 0 ? Math.round(prices.reduce((s, p) => s + p, 0) / prices.length) : 0;
    a.listingCount = a.listings.length;
    // "Investor-friendly" = avg list price is at or below median for the area
    a.investorFriendly = a.avgListPrice > 0 && a.avgListPrice <= medianPrice;
  });

  // Sort: investor-friendly first, then by listing count
  agents.sort((a, b) => {
    if (a.investorFriendly && !b.investorFriendly) return -1;
    if (!a.investorFriendly && b.investorFriendly) return 1;
    return b.listingCount - a.listingCount;
  });

  // Debug: scan entire response for any phone-containing keys
  const phoneKeyFinds = [];
  function scanForPhoneKeys(obj, path, depth) {
    if (!obj || depth > 6) return;
    if (typeof obj === 'string' && phoneRegex.test(obj)) {
      phoneKeyFinds.push({ path, value: obj.substring(0, 50) });
      return;
    }
    if (typeof obj === 'number' && String(obj).length >= 10 && String(obj).length <= 12) {
      phoneKeyFinds.push({ path, value: String(obj) });
      return;
    }
    if (Array.isArray(obj)) {
      obj.slice(0, 5).forEach((v, i) => scanForPhoneKeys(v, path + '[' + i + ']', depth + 1));
    } else if (typeof obj === 'object') {
      for (const [k, v] of Object.entries(obj)) {
        scanForPhoneKeys(v, path + '.' + k, depth + 1);
      }
    }
  }
  scanForPhoneKeys(propertyData, 'root', 0);

  return {
    agents: agents.slice(0, 20),
    totalAgentsFound: agents.length,
    medianAreaPrice: medianPrice,
    totalListingsScanned: allPrices.length,
    subjectAddress: address,
    _debug: {
      rawAgentsExtracted: rawAgents.length,
      agentsWithPhone: rawAgents.filter(a => a.agentPhone).length,
      agentsWithBrokerPhone: rawAgents.filter(a => a.brokerPhone).length,
      phoneKeysFoundInResponse: phoneKeyFinds.slice(0, 30),
    },
  };
}

/**
 * Deep-extract all property-like objects from a Zillow API response.
 * Searches recursively for arrays/objects that look like property records
 * (have address/zpid/price fields). Used to find embedded comps, nearby sales, etc.
 */
function deepExtractProperties(obj, depth, maxDepth, subjectZpid) {
  if (!obj || depth > maxDepth) return [];
  const results = [];
  const seen = new Set();

  function looksLikeProperty(item) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) return false;
    const hasAddr = item.address || item.streetAddress || item.formattedAddress;
    const hasZpid = item.zpid;
    const hasSoldPrice = item.soldPrice || item.lastSoldPrice || item.lastSalePrice || item.salePrice;
    const hasPrice = hasSoldPrice || item.price;
    const hasBeds = item.bedrooms || item.beds;

    // Filter out rental listings — these have rent prices, not sale prices
    const status = (item.homeStatus || item.statusType || item.listingStatus || item.status || '').toString().toLowerCase();
    if (status.includes('for_rent') || status.includes('rental') || status === 'rent') return false;

    // If it only has a 'price' (no sold-specific fields) and the price is suspiciously low
    // (under $5,000/mo range), it's probably a rent listing
    if (!hasSoldPrice && item.price && item.price < 10000 && !item.lastSoldDate && !item.dateSold) return false;

    return (hasAddr || hasZpid) && (hasPrice || hasBeds);
  }

  function extract(node, d) {
    if (!node || d > maxDepth) return;
    if (Array.isArray(node)) {
      node.forEach(item => {
        if (looksLikeProperty(item)) {
          const id = item.zpid || (item.address || item.streetAddress || '') + '_' + (item.price || item.soldPrice || '');
          if (id && !seen.has(id) && item.zpid !== subjectZpid) {
            seen.add(id);
            results.push(item);
          }
        }
        extract(item, d + 1);
      });
    } else if (typeof node === 'object') {
      // Check known comp/nearby keys first (higher priority)
      const compKeys = ['comps', 'comparables', 'nearbyHomes', 'nearbySales', 'recentlySold',
        'similarHomes', 'nearbyProperties', 'soldNearby', 'nearbyAssessments',
        'homeRecommendations', 'recommendations', 'listResults', 'results', 'props',
        'searchResults', 'categoryTotals', 'relaxedResults', 'mapResults'];
      for (const key of compKeys) {
        if (node[key]) extract(node[key], d + 1);
      }
      // Then check all other keys
      Object.entries(node).forEach(([key, val]) => {
        if (!compKeys.includes(key) && val && typeof val === 'object') {
          if (looksLikeProperty(val)) {
            const id = val.zpid || (val.address || val.streetAddress || '') + '_' + (val.price || val.soldPrice || '');
            if (id && !seen.has(id) && val.zpid !== subjectZpid) {
              seen.add(id);
              results.push(val);
            }
          }
          extract(val, d + 1);
        }
      });
    }
  }

  extract(obj, 0);
  return results;
}

/**
 * Full property lookup: get property by address, then search for comps nearby
 * Returns a combined payload for the Dispo tab
 */
async function zillowFullLookup(address, env) {
  // Step 1: Get property details by address
  let propertyData = null;
  try {
    propertyData = await zillowGetByAddress(address, env);
  } catch (e) {
    return {
      found: false,
      address: address,
      message: 'Zillow API error: ' + e.message,
      error: e.message,
    };
  }

  // The ZLLW Working API returns property data directly or nested
  // Handle various response shapes
  const p = propertyData?.propertyDetails || propertyData?.data || propertyData || {};

  if (!p || (!p.zpid && !p.address && !p.streetAddress && !p.bedrooms)) {
    return {
      found: false,
      address: address,
      message: 'No Zillow data found for this address. Try the full street address with city, state, zip.',
      rawResponse: propertyData,
    };
  }

  const zpid = p.zpid || null;

  // Step 2: Extract comps from property response using deep extraction
  // The ZLLW API embeds nearby sales, comps, similar homes in the property response
  const extractedProperties = deepExtractProperties(propertyData, 0, 6, zpid);

  // Extract photos from the property data
  // Recursively search for photo URLs in the response
  const photoUrls = [];
  const seenUrls = new Set();

  function extractPhotosFromObj(obj, depth) {
    if (!obj || depth > 5) return;
    if (typeof obj === 'string') {
      // Check if it looks like an image URL
      if ((obj.includes('zillowstatic.com') || obj.includes('zillow.com') || obj.includes('.jpg') || obj.includes('.jpeg') || obj.includes('.png') || obj.includes('.webp')) && obj.startsWith('http') && !seenUrls.has(obj)) {
        seenUrls.add(obj);
        photoUrls.push(obj);
      }
      return;
    }
    if (Array.isArray(obj)) {
      obj.forEach(item => extractPhotosFromObj(item, depth + 1));
      return;
    }
    if (typeof obj === 'object') {
      // Prioritize known photo fields
      const photoKeys = ['photos', 'images', 'responsivePhotos', 'hugePhotos', 'hiResImageLink', 'originalPhotos', 'photoGallery', 'media', 'photoUrls', 'imgSrc', 'url', 'fullUrl', 'highResImageLink'];
      photoKeys.forEach(key => {
        if (obj[key] !== undefined) {
          extractPhotosFromObj(obj[key], depth + 1);
        }
      });
      // Also check mixedSources pattern (Zillow nested photo format)
      if (obj.mixedSources) {
        const jpegSources = obj.mixedSources.jpeg || obj.mixedSources.webp || [];
        if (Array.isArray(jpegSources) && jpegSources.length > 0) {
          const best = jpegSources[jpegSources.length - 1];
          if (best && best.url && !seenUrls.has(best.url)) {
            seenUrls.add(best.url);
            photoUrls.push(best.url);
          }
        }
      }
      // Check for direct image URL patterns in any field
      Object.entries(obj).forEach(([key, val]) => {
        if (typeof val === 'string' && !photoKeys.includes(key)) {
          extractPhotosFromObj(val, depth + 1);
        } else if (Array.isArray(val) && ['photos','images','responsivePhotos','originalPhotos','photoGallery','media'].includes(key)) {
          // Already handled above
        } else if (Array.isArray(val) && depth < 3) {
          extractPhotosFromObj(val, depth + 1);
        }
      });
    }
  }

  extractPhotosFromObj(propertyData, 0);

  // Build comps from extracted properties
  const comps = [];
  const seenCompZpids = new Set();

  extractedProperties.forEach(comp => {
    if (!comp || typeof comp !== 'object') return;
    const cZpid = comp.zpid;
    if (cZpid && seenCompZpids.has(cZpid)) return;
    if (cZpid) seenCompZpids.add(cZpid);
    comps.push({
      address: (typeof comp.address === 'object' && comp.address ? (comp.address.streetAddress || comp.address.street || '') + (comp.address.city ? ', ' + comp.address.city : '') : null) || comp.streetAddress || comp.formattedAddress || comp.address || '',
      price: comp.soldPrice || comp.lastSoldPrice || comp.lastSalePrice || comp.salePrice || comp.price || 0,
      bedrooms: comp.bedrooms || comp.beds || null,
      bathrooms: comp.bathrooms || comp.baths || null,
      livingArea: comp.livingArea || comp.livingAreaValue || comp.sqft || null,
      lotSize: comp.lotSize || comp.lotAreaValue || null,
      yearBuilt: comp.yearBuilt || null,
      soldDate: comp.dateSold || comp.lastSoldDate || comp.datePosted || null,
      distance: comp.distance || null,
      zpid: cZpid || null,
      imgSrc: comp.imgSrc || comp.miniCardPhotos?.[0]?.url || null,
    });
  });

  // Build clean summary
  const summary = {
    found: true,
    zpid: zpid,
    address: p.address || p.streetAddress || address,
    fullAddress: [p.streetAddress || p.address, p.city, p.state, p.zipcode].filter(Boolean).join(', '),
    bedrooms: p.bedrooms || p.beds || null,
    bathrooms: p.bathrooms || p.baths || null,
    livingArea: p.livingArea || p.livingAreaValue || p.sqft || null,
    lotSize: p.lotAreaValue || p.lotSize || null,
    lotUnit: p.lotAreaUnit || 'sqft',
    yearBuilt: p.yearBuilt || null,
    propertyType: p.homeType || p.propertyType || p.homeTypeDimension || null,
    zestimate: p.zestimate || null,
    rentZestimate: p.rentZestimate || null,
    price: p.price || null,
    taxAssessedValue: p.taxAssessedValue || null,
    description: p.description || null,
    latitude: p.latitude || null,
    longitude: p.longitude || null,
    photos: photoUrls,
    photoCount: photoUrls.length,
    comps: comps,
    rawProperty: propertyData,
    _debug: {
      topLevelKeys: Object.keys(propertyData || {}),
      nestedKeys: Object.keys(p || {}).slice(0, 40),
      photoFieldsFound: ['photos','images','responsivePhotos','hugePhotos','hiResImageLink','originalPhotos','photoGallery','media','imgSrc']
        .filter(k => p[k] !== undefined || (propertyData && propertyData[k] !== undefined)),
      compFieldsFound: ['comps','comparables','nearbyHomes','nearbySales','recentlySold','similarHomes','nearbyProperties']
        .filter(k => p[k] !== undefined || (propertyData && propertyData[k] !== undefined)),
      extractedPropertyCount: extractedProperties.length,
    },
  };

  return summary;
}

// ==================== INTELLIGENT COMP FINDER ====================

/**
 * Haversine distance between two lat/lng points in miles
 */
function haversineDistance(lat1, lon1, lat2, lon2) {
  const R = 3959; // Earth radius in miles
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon/2) * Math.sin(dLon/2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * Score a potential comp against the subject property (0-100)
 * Higher = better match
 */
function scoreComp(subject, comp) {
  let score = 0;
  let maxScore = 0;
  const penalties = [];

  // --- Sqft similarity (25 pts) ---
  maxScore += 25;
  const sSqft = subject.livingArea || 0;
  const cSqft = comp.livingArea || comp.sqft || 0;
  if (sSqft > 0 && cSqft > 0) {
    const sqftRatio = Math.min(sSqft, cSqft) / Math.max(sSqft, cSqft);
    if (sqftRatio >= 0.85) score += 25;
    else if (sqftRatio >= 0.75) score += 18;
    else if (sqftRatio >= 0.60) score += 10;
    else { score += 3; penalties.push('Sqft diff >' + Math.round((1-sqftRatio)*100) + '%'); }
  }

  // --- Bed count (15 pts) ---
  maxScore += 15;
  const sBeds = subject.bedrooms || 0;
  const cBeds = comp.bedrooms || comp.beds || 0;
  if (sBeds > 0 && cBeds > 0) {
    const bedDiff = Math.abs(sBeds - cBeds);
    if (bedDiff === 0) score += 15;
    else if (bedDiff === 1) score += 10;
    else { score += 3; penalties.push(bedDiff + ' bed diff'); }
  }

  // --- Bath count (10 pts) ---
  maxScore += 10;
  const sBaths = subject.bathrooms || 0;
  const cBaths = comp.bathrooms || comp.baths || 0;
  if (sBaths > 0 && cBaths > 0) {
    const bathDiff = Math.abs(sBaths - cBaths);
    if (bathDiff === 0) score += 10;
    else if (bathDiff <= 1) score += 7;
    else { score += 2; penalties.push(bathDiff + ' bath diff'); }
  }

  // --- Lot size similarity (10 pts) ---
  maxScore += 10;
  const sLot = subject.lotSize || 0;
  const cLot = comp.lotSize || comp.lotAreaValue || 0;
  if (sLot > 0 && cLot > 0) {
    const lotRatio = Math.min(sLot, cLot) / Math.max(sLot, cLot);
    if (lotRatio >= 0.70) score += 10;
    else if (lotRatio >= 0.50) score += 6;
    else { score += 2; penalties.push('Lot size diff'); }
  }

  // --- Year built (10 pts) ---
  maxScore += 10;
  const sYear = subject.yearBuilt || 0;
  const cYear = comp.yearBuilt || 0;
  if (sYear > 0 && cYear > 0) {
    const yearDiff = Math.abs(sYear - cYear);
    if (yearDiff <= 5) score += 10;
    else if (yearDiff <= 15) score += 7;
    else if (yearDiff <= 30) score += 4;
    else { score += 1; penalties.push(yearDiff + 'yr age diff'); }
  }

  // --- Distance (20 pts) ---
  maxScore += 20;
  const dist = comp._distance || 999;
  if (dist <= 0.25) score += 20;
  else if (dist <= 0.5) score += 17;
  else if (dist <= 1.0) score += 13;
  else if (dist <= 2.0) score += 8;
  else if (dist <= 5.0) score += 4;
  else { score += 1; penalties.push(dist.toFixed(1) + ' mi away'); }

  // --- Property type match (5 pts) ---
  maxScore += 5;
  const sType = (subject.propertyType || subject.homeType || '').toLowerCase();
  const cType = (comp.homeType || comp.propertyType || '').toLowerCase();
  if (sType && cType && (sType.includes(cType) || cType.includes(sType) || sType === cType)) {
    score += 5;
  } else if (sType && cType) {
    penalties.push('Type: ' + cType);
  }

  // --- Recency bonus (5 pts) ---
  maxScore += 5;
  const soldDate = comp.dateSold || comp.lastSoldDate || comp.datePosted || null;
  if (soldDate) {
    const daysAgo = Math.floor((Date.now() - new Date(soldDate).getTime()) / (1000 * 60 * 60 * 24));
    if (daysAgo <= 30) score += 5;
    else if (daysAgo <= 60) score += 3;
    else if (daysAgo <= 90) score += 2;
    else score += 1;
  }

  const pct = maxScore > 0 ? Math.round((score / maxScore) * 100) : 0;

  return {
    score: score,
    maxScore: maxScore,
    matchPct: pct,
    penalties: penalties,
    grade: pct >= 85 ? 'A' : pct >= 70 ? 'B' : pct >= 55 ? 'C' : pct >= 40 ? 'D' : 'F',
  };
}

/**
 * Extract the best sale price and date from a property's priceHistory.
 * In non-disclosure states (TX, NM, UT, etc.), "Sold" events have no price —
 * use the Pending/Listed price that preceded the sold event instead.
 * Returns { price, date, source } or null.
 */
function extractSalePriceFromHistory(priceHistory) {
  if (!Array.isArray(priceHistory) || priceHistory.length === 0) return null;
  const sorted = [...priceHistory].sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));

  // Pass 1: find the most recent Sold event with a price
  for (const evt of sorted) {
    const t = (evt.event || '').toLowerCase();
    if ((t === 'sold' || t.includes('sold')) && evt.price && evt.price > 10000) {
      return { price: evt.price, date: evt.date || null, source: 'history:sold' };
    }
  }

  // Pass 2: Sold event exists but has no price (non-disclosure state)
  // Find the Pending/Listed price that preceded the most recent sold
  for (let i = 0; i < sorted.length; i++) {
    const t = (sorted[i].event || '').toLowerCase();
    if (t === 'sold' || t.includes('sold')) {
      // Walk to older entries to find preceding pending/listed price
      for (let j = i + 1; j < sorted.length; j++) {
        const jt = (sorted[j].event || '').toLowerCase();
        if ((jt.includes('pending') || jt.includes('contingent')) && sorted[j].price && sorted[j].price > 10000) {
          return { price: sorted[j].price, date: sorted[i].date || sorted[j].date || null, source: 'history:pending_before_sold' };
        }
        if ((jt.includes('listed') || jt.includes('price change')) && sorted[j].price && sorted[j].price > 10000) {
          return { price: sorted[j].price, date: sorted[i].date || sorted[j].date || null, source: 'history:listed_before_sold' };
        }
      }
      // Sold event found but no preceding price — use its date at least
      break;
    }
  }

  // Pass 3: No Sold event — look for Pending or Contingent (in-contract, about to close)
  for (const evt of sorted) {
    const t = (evt.event || '').toLowerCase();
    if ((t.includes('pending') || t.includes('contingent')) && evt.price && evt.price > 10000) {
      return { price: evt.price, date: evt.date || null, source: 'history:pending' };
    }
  }

  return null;
}

/**
 * Find comps for a subject property — improved for non-disclosure states (TX, etc.)
 *
 * Strategy:
 * 1. Get subject property details
 * 2. Deep-extract embedded properties from the response
 * 3. Try a location-based search for recently sold homes in the same zip
 * 4. Fetch EVERY extracted zpid individually (critical for TX — the individual
 *    property lookup returns priceHistory with actual pending/sold prices,
 *    while the embedded stubs only have Zestimates)
 * 5. Use extractSalePriceFromHistory to find real prices
 * 6. Score, rank, and return
 */
async function findComps(address, daysBack, env) {
  // Step 1: Get subject property details
  let propertyData;
  try {
    propertyData = await zillowGetByAddress(address, env);
  } catch (e) {
    return { error: 'Could not fetch subject property: ' + e.message };
  }

  const p = propertyData?.propertyDetails || propertyData?.data || propertyData || {};
  if (!p || (!p.zpid && !p.address && !p.bedrooms)) {
    return { error: 'No Zillow data found for this address', rawResponse: propertyData };
  }

  // Extract subject photos
  const subjectPhotos = [];
  const photoSources = p.photos || p.responsivePhotos || p.hugePhotos || p.hiResPhotos || [];
  if (Array.isArray(photoSources)) {
    photoSources.forEach(photo => {
      if (typeof photo === 'string') { subjectPhotos.push(photo); }
      else if (photo?.url) { subjectPhotos.push(photo.url); }
      else if (photo?.mixedSources?.jpeg) {
        const jpegs = photo.mixedSources.jpeg;
        const largest = jpegs.reduce((best, cur) => ((cur.width || 0) > (best.width || 0) ? cur : best), jpegs[0]);
        if (largest?.url) subjectPhotos.push(largest.url);
      }
    });
  }

  // Extract subject's last sale price using the improved helper
  let lastSalePrice = p.lastSoldPrice || p.lastSalePrice || p.soldPrice || p.salePrice || null;
  let lastSaleDate = p.dateSold || p.lastSoldDate || p.dateSoldString || null;

  if ((!lastSalePrice || !lastSaleDate) && Array.isArray(p.priceHistory)) {
    const histResult = extractSalePriceFromHistory(p.priceHistory);
    if (histResult) {
      if (!lastSalePrice) lastSalePrice = histResult.price;
      if (!lastSaleDate) lastSaleDate = histResult.date;
    }
  }

  // Tax history fallback
  if (!lastSalePrice && Array.isArray(p.taxHistory) && p.taxHistory.length > 0) {
    const sorted = [...p.taxHistory].sort((a, b) => (b.time || 0) - (a.time || 0));
    if (sorted[0] && sorted[0].value) lastSalePrice = sorted[0].value;
  }

  const subject = {
    zpid: p.zpid,
    address: p.address || p.streetAddress || address,
    bedrooms: p.bedrooms || p.beds || null,
    bathrooms: p.bathrooms || p.baths || null,
    livingArea: p.livingArea || p.livingAreaValue || p.sqft || null,
    lotSize: p.lotAreaValue || p.lotSize || null,
    yearBuilt: p.yearBuilt || null,
    propertyType: p.homeType || p.propertyType || null,
    latitude: p.latitude || null,
    longitude: p.longitude || null,
    zestimate: p.zestimate || null,
    rentZestimate: p.rentZestimate || null,
    zipcode: p.zipcode || p.zip || null,
    city: p.city || null,
    state: p.state || null,
    imgSrc: p.imgSrc || p.streetViewTileImageUrlMediumAddress || (subjectPhotos.length > 0 ? subjectPhotos[0] : null),
    photos: subjectPhotos.slice(0, 30),
    lastSalePrice: lastSalePrice || null,
    lastSaleDate: lastSaleDate || null,
  };

  // Step 2: Deep-extract all property-like objects from the subject response
  const extractedProperties = deepExtractProperties(propertyData, 0, 6, subject.zpid);

  // Step 2b: Try location-based search for recently sold homes in the same zip
  let searchExtracted = [];
  const zipOrCity = subject.zipcode || (subject.city && subject.state ? `${subject.city}, ${subject.state}` : null);
  if (zipOrCity) {
    try {
      const searchData = await zillowSearchByLocation(zipOrCity, 'RecentlySold', env);
      if (searchData) {
        searchExtracted = deepExtractProperties(searchData, 0, 4, subject.zpid);
      }
    } catch (e) { /* search endpoint not available — that's fine */ }
  }

  // Merge all extracted properties, dedup by zpid
  const allExtracted = [];
  const seenExtractedZpids = new Set();
  [...extractedProperties, ...searchExtracted].forEach(ep => {
    const id = ep.zpid || (ep.address || ep.streetAddress || '') + '_' + (ep.price || ep.soldPrice || '');
    if (id && !seenExtractedZpids.has(id)) {
      seenExtractedZpids.add(id);
      allExtracted.push(ep);
    }
  });

  // Step 3: Fetch ALL zpids individually for full property data
  // KEY INSIGHT: In non-disclosure states (TX), embedded property stubs only have
  // Zestimates. The individual /pro/byzpid lookup returns priceHistory with actual
  // pending/sold prices. So we fetch ALL zpids, not just ones without prices.
  const zpidsToFetch = [];
  const fullDataProps = [];
  const seenFetchZpids = new Set();
  const stubsByZpid = new Map(); // Keep stubs as fallback if fetch fails

  allExtracted.forEach(ep => {
    const hasSoldPrice = ep.soldPrice || ep.lastSoldPrice || ep.lastSalePrice;
    const hasBasicData = (ep.livingArea || ep.sqft || ep.bedrooms || ep.beds);
    const hasPriceHistory = Array.isArray(ep.priceHistory) && ep.priceHistory.length > 0;

    if (hasSoldPrice && hasBasicData && hasPriceHistory) {
      // Has everything — use as-is (no need to refetch)
      fullDataProps.push(ep);
    } else if (ep.zpid && !seenFetchZpids.has(ep.zpid)) {
      // Queue for individual fetch to get priceHistory + full details
      zpidsToFetch.push(ep.zpid);
      seenFetchZpids.add(ep.zpid);
      // Save the stub so we can fall back to it if the fetch fails
      stubsByZpid.set(String(ep.zpid), ep);
    } else if (!ep.zpid && hasSoldPrice && hasBasicData) {
      fullDataProps.push(ep);
    }
  });

  // Fetch zpids individually with retry logic
  // Batch in groups of 3 (smaller batches = less rate limiting)
  // Retry failed fetches once with a delay
  const maxFetch = 25;
  const fetchedZpids = new Set(); // track which zpids we successfully fetched

  if (zpidsToFetch.length > 0) {
    const toFetch = zpidsToFetch.slice(0, maxFetch);
    const batchSize = 3;
    const failedZpids = [];

    for (let i = 0; i < toFetch.length; i += batchSize) {
      const batch = toFetch.slice(i, i + batchSize);
      const fetched = await Promise.allSettled(
        batch.map(zpid => zillowGetByZpid(zpid, env))
      );
      fetched.forEach((result, idx) => {
        const zpid = batch[idx];
        if (result.status === 'fulfilled' && result.value) {
          const fd = result.value?.propertyDetails || result.value?.data || result.value || {};
          if (fd.zpid || fd.address || fd.streetAddress) {
            fullDataProps.push(fd);
            fetchedZpids.add(String(zpid));
          } else {
            failedZpids.push(zpid);
          }
        } else {
          failedZpids.push(zpid);
        }
      });
      // Small delay between batches to avoid rate limiting
      if (i + batchSize < toFetch.length) {
        await new Promise(r => setTimeout(r, 300));
      }
    }

    // Retry failed fetches once with a delay
    if (failedZpids.length > 0) {
      await new Promise(r => setTimeout(r, 1000));
      for (let i = 0; i < failedZpids.length; i += batchSize) {
        const batch = failedZpids.slice(i, i + batchSize);
        const retried = await Promise.allSettled(
          batch.map(zpid => zillowGetByZpid(zpid, env))
        );
        retried.forEach((result, idx) => {
          const zpid = batch[idx];
          if (result.status === 'fulfilled' && result.value) {
            const fd = result.value?.propertyDetails || result.value?.data || result.value || {};
            if (fd.zpid || fd.address || fd.streetAddress) {
              fullDataProps.push(fd);
              fetchedZpids.add(String(zpid));
            }
          }
        });
        if (i + batchSize < failedZpids.length) {
          await new Promise(r => setTimeout(r, 300));
        }
      }
    }

    // For any zpids that STILL failed to fetch, use the original stub data as fallback
    // This ensures we at least show properties with Zestimate prices (labeled accordingly)
    // rather than losing them entirely
    for (const [zpidStr, stub] of stubsByZpid.entries()) {
      if (!fetchedZpids.has(zpidStr)) {
        fullDataProps.push(stub);
      }
    }
  }

  // Step 4: Process all properties into scored comps
  const cutoffDate = new Date(Date.now() - (daysBack || 180) * 24 * 60 * 60 * 1000);
  const scoredComps = [];
  const seenZpids = new Set();

  fullDataProps.forEach(comp => {
    if (!comp || typeof comp !== 'object') return;
    if (comp.zpid && comp.zpid === subject.zpid) return;
    if (comp.zpid && seenZpids.has(comp.zpid)) return;
    if (comp.zpid) seenZpids.add(comp.zpid);

    // Filter out rental listings
    const compStatus = (comp.homeStatus || comp.statusType || comp.listingStatus || comp.status || '').toString().toLowerCase();
    if (compStatus.includes('for_rent') || compStatus.includes('rental') || compStatus === 'rent') return;

    // === PRICE EXTRACTION (improved for non-disclosure states) ===
    let price = 0;
    let priceSource = null;
    let soldDateStr = comp.dateSold || comp.lastSoldDate || comp.dateSoldString || null;

    // Priority 1: Explicit sold price fields
    price = comp.soldPrice || comp.lastSoldPrice || comp.lastSalePrice || 0;
    if (price > 0) {
      priceSource = comp.soldPrice ? 'soldPrice' : comp.lastSoldPrice ? 'lastSoldPrice' : 'lastSalePrice';
    }

    // Priority 2: priceHistory — the gold standard for non-disclosure states
    if (Array.isArray(comp.priceHistory) && comp.priceHistory.length > 0) {
      const histResult = extractSalePriceFromHistory(comp.priceHistory);
      if (histResult) {
        // If we got a price from priceHistory and either:
        // - we had no price before, OR
        // - the existing price looks like a Zestimate (matches comp.zestimate or comp.price but not a sold field)
        const existingIsZestimate = price > 0 && !comp.soldPrice && !comp.lastSoldPrice && !comp.lastSalePrice
          && (price === comp.zestimate || price === comp.price);
        if (price <= 0 || existingIsZestimate) {
          price = histResult.price;
          priceSource = histResult.source;
        }
        if (!soldDateStr && histResult.date) soldDateStr = histResult.date;
      }
    }

    // Priority 3: If no priceHistory result, check generic price + status
    if (price <= 0 && (comp.price || comp.salePrice)) {
      const isActiveListing = compStatus.includes('for_sale') || compStatus.includes('coming_soon')
        || compStatus.includes('new_listing') || compStatus.includes('active');
      const isRental = compStatus.includes('for_rent') || compStatus.includes('rental') || compStatus === 'rent';

      if (!isActiveListing && !isRental) {
        price = comp.salePrice || comp.price;
        priceSource = 'price:' + (compStatus || 'inferred');
      }
    }

    // Skip if no real price found
    if (price <= 0) return;
    // Skip if price looks like a monthly rent
    if (price < 10000 && !soldDateStr) return;

    // Skip active for-sale listings (we want sold/pending/recently sold comps)
    const isActiveListing2 = compStatus.includes('for_sale') || compStatus.includes('coming_soon')
      || compStatus.includes('new_listing');
    if (isActiveListing2 && !soldDateStr) return;

    // ZESTIMATE DETECTION: Flag likely Zestimates but DON'T filter them out —
    // in non-disclosure states (TX) these may be the only data available.
    // Instead, label them and heavily penalize their match score.
    const isLikelyZestimate = comp.zestimate && Math.abs(price - comp.zestimate) < 100 && !soldDateStr
      && (!priceSource || priceSource.startsWith('price:'));
    if (isLikelyZestimate) {
      priceSource = 'zestimate';
    }

    // Date filter — allow wider window for non-disclosure states
    if (soldDateStr) {
      const soldDate = new Date(soldDateStr);
      if (!isNaN(soldDate.getTime()) && soldDate < cutoffDate) return;
    }

    // Calculate distance
    let distance = null;
    if (subject.latitude && subject.longitude && comp.latitude && comp.longitude) {
      distance = haversineDistance(subject.latitude, subject.longitude, comp.latitude, comp.longitude);
      if (distance > 5) return;
    }

    comp._distance = distance;
    const scoring = scoreComp(subject, comp);

    // Adjust match score based on price source quality
    let adjustedMatchPct = scoring.matchPct;

    // Heavy penalty for Zestimate-only prices (not real sale data)
    if (priceSource === 'zestimate') {
      adjustedMatchPct = Math.max(0, adjustedMatchPct - 20);
    }
    // Moderate penalty for generic price with unknown source
    else if (priceSource && priceSource.startsWith('price:')) {
      adjustedMatchPct = Math.max(0, adjustedMatchPct - 10);
    }
    // Boost for verified sale prices
    if (priceSource && (priceSource.includes('sold') || priceSource.includes('pending_before_sold'))) {
      adjustedMatchPct = Math.min(100, adjustedMatchPct + 5);
    }

    scoredComps.push({
      address: (typeof comp.address === 'object' && comp.address ? (comp.address.streetAddress || comp.address.street || '') + (comp.address.city ? ', ' + comp.address.city : '') : null) || comp.streetAddress || comp.formattedAddress || comp.address || '',
      price: price,
      bedrooms: comp.bedrooms || comp.beds || null,
      bathrooms: comp.bathrooms || comp.baths || null,
      livingArea: comp.livingArea || comp.livingAreaValue || comp.sqft || null,
      lotSize: comp.lotSize || comp.lotAreaValue || null,
      yearBuilt: comp.yearBuilt || null,
      propertyType: comp.homeType || comp.propertyType || null,
      homeStatus: comp.homeStatus || comp.statusType || comp.listingStatus || null,
      soldDate: soldDateStr,
      latitude: comp.latitude || null,
      longitude: comp.longitude || null,
      distance: distance ? Math.round(distance * 100) / 100 : null,
      zpid: comp.zpid || null,
      imgSrc: comp.imgSrc || comp.miniCardPhotos?.[0]?.url || null,
      _priceSource: priceSource || 'unknown',
      matchPct: adjustedMatchPct,
      grade: adjustedMatchPct >= 85 ? 'A' : adjustedMatchPct >= 70 ? 'B' : adjustedMatchPct >= 55 ? 'C' : adjustedMatchPct >= 40 ? 'D' : 'F',
      scoreBreakdown: scoring,
    });
  });

  // Sort by match percentage (best first), then by verified price source
  scoredComps.sort((a, b) => {
    // First by match %
    if (b.matchPct !== a.matchPct) return b.matchPct - a.matchPct;
    // Tiebreak: prefer verified prices
    const aVerified = a._priceSource && (a._priceSource.includes('sold') || a._priceSource.includes('pending'));
    const bVerified = b._priceSource && (b._priceSource.includes('sold') || b._priceSource.includes('pending'));
    if (aVerified && !bVerified) return -1;
    if (bVerified && !aVerified) return 1;
    return 0;
  });

  // Calculate ARV from top comps — prefer verified-price comps
  const verifiedComps = scoredComps.filter(c => c._priceSource && (c._priceSource.includes('sold') || c._priceSource.includes('pending') || c._priceSource.includes('lastSoldPrice') || c._priceSource.includes('soldPrice')));
  const topComps = verifiedComps.length >= 3 ? verifiedComps.slice(0, 6) :
    scoredComps.filter(c => c.grade === 'A' || c.grade === 'B').slice(0, 6);
  const arvComps = topComps.length >= 3 ? topComps : scoredComps.slice(0, 6);
  const arvPrices = arvComps.map(c => c.price).filter(p => p > 0);
  const avgARV = arvPrices.length > 0 ? Math.round(arvPrices.reduce((a, b) => a + b, 0) / arvPrices.length) : null;
  const medianARV = arvPrices.length > 0 ? [...arvPrices].sort((a, b) => a - b)[Math.floor(arvPrices.length / 2)] : null;

  return {
    subject: subject,
    comps: scoredComps,
    compCount: scoredComps.length,
    totalExtracted: allExtracted.length,
    zpidsFetched: Math.min(zpidsToFetch.length, maxFetch),
    fullDataCount: fullDataProps.length,
    searchExtractedCount: searchExtracted.length,
    daysBack: daysBack || 180,
    suggestedARV: avgARV,
    medianARV: medianARV,
    arvCompsUsed: arvComps.length,
    verifiedPriceComps: verifiedComps.length,
    zestimate: subject.zestimate,
    generatedAt: new Date().toISOString(),
    _debug: {
      topLevelKeys: Object.keys(propertyData || {}),
      extractedFromSubject: extractedProperties.length,
      extractedFromSearch: searchExtracted.length,
      totalExtracted: allExtracted.length,
      extractedWithZpid: allExtracted.filter(e => e.zpid).length,
      extractedWithSoldPrice: allExtracted.filter(e => e.soldPrice || e.lastSoldPrice || e.lastSalePrice).length,
      extractedWithPriceHistory: allExtracted.filter(e => Array.isArray(e.priceHistory) && e.priceHistory.length > 0).length,
      extractedWithGenericPrice: allExtracted.filter(e => e.price && !e.soldPrice && !e.lastSoldPrice && !e.lastSalePrice).length,
      zpidsQueuedForFetch: zpidsToFetch.length,
      zpidsFetchedSuccessfully: fetchedZpids.size,
      zpidsFetchFailed: zpidsToFetch.slice(0, maxFetch).length - fetchedZpids.size,
      stubsUsedAsFallback: zpidsToFetch.slice(0, maxFetch).length - fetchedZpids.size,
      fullDataAfterFetch: fullDataProps.length,
      fullDataWithPriceHistory: fullDataProps.filter(f => Array.isArray(f.priceHistory) && f.priceHistory.length > 0).length,
      verifiedPriceComps: verifiedComps.length,
      filteredOut: fullDataProps.length - scoredComps.length,
      compPriceSources: scoredComps.reduce((acc, c) => { acc[c._priceSource] = (acc[c._priceSource] || 0) + 1; return acc; }, {}),
      extractedSample: allExtracted.slice(0, 5).map(e => ({
        zpid: e.zpid || null,
        address: (typeof e.address === 'object' ? JSON.stringify(e.address).substring(0, 50) : (e.address || e.streetAddress || '')).toString().substring(0, 50),
        soldPrice: e.soldPrice || null,
        lastSoldPrice: e.lastSoldPrice || null,
        price: e.price || null,
        zestimate: e.zestimate || null,
        homeStatus: e.homeStatus || e.statusType || null,
        hasPriceHistory: Array.isArray(e.priceHistory) && e.priceHistory.length > 0,
      })),
    },
  };
}

/**
 * CORS headers
 */
function getCorsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
}

/**
 * JSON response helper
 */
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      ...getCorsHeaders(),
    },
  });
}

/**
 * Error response helper
 */
function errorResponse(message, status = 400) {
  return jsonResponse({ error: message }, status);
}

/**
 * Main Worker request handler
 */
export default {
  async fetch(request, env) {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: getCorsHeaders(),
      });
    }

    const url = new URL(request.url);
    const pathname = url.pathname;

    // Allow GET and POST (POST needed for photo uploads)
    if (request.method !== 'GET' && request.method !== 'POST') {
      return errorResponse('Method not allowed', 405);
    }

    try {
      // Validate environment variables
      if (!env.GHL_API_KEY) {
        return errorResponse('GHL_API_KEY not configured', 500);
      }
      if (!env.GHL_LOCATION_ID) {
        return errorResponse('GHL_LOCATION_ID not configured', 500);
      }

      // Route handling
      if (pathname === '/api/pipelines') {
        const data = await fetchPipelines(env);
        return jsonResponse(data);
      }

      if (pathname === '/api/opportunities') {
        const data = await fetchAllOpportunities(env);
        return jsonResponse(data);
      }

      if (pathname === '/api/pipeline-stats') {
        const stats = await computePipelineStats(env);
        return jsonResponse(stats);
      }

      if (pathname === '/api/lead-sources') {
        const monthsParam = url.searchParams.get('months');
        const monthsBack = monthsParam ? parseInt(monthsParam) : null;
        const stats = await computeLeadSourceStats(env, monthsBack);
        return jsonResponse(stats);
      }

      if (pathname === '/api/debug') {
        const [pipelinesData, opportunitiesData] = await Promise.all([
          fetchPipelines(env),
          fetchAllOpportunities(env),
        ]);
        const pipelines = pipelinesData.pipelines || [];
        const opps = opportunitiesData.opportunities || [];
        return jsonResponse({
          pipelineCount: pipelines.length,
          pipelines: pipelines.map(p => ({
            id: p.id,
            name: p.name,
            stageCount: (p.stages || []).length,
            stages: (p.stages || []).map(s => ({ id: s.id, name: s.name })),
          })),
          opportunityCount: opps.length,
          sampleOpportunity: opps.length > 0 ? opps[0] : null,
          opportunityKeys: opps.length > 0 ? Object.keys(opps[0]) : [],
        });
      }

      // ---- Zillow API Routes ----

      if (pathname === '/api/zillow/property') {
        const address = url.searchParams.get('address');
        const zpid = url.searchParams.get('zpid');
        if (!address && !zpid) return errorResponse('address or zpid parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        const data = zpid ? await zillowGetByZpid(zpid, env) : await zillowGetByAddress(address, env);
        return jsonResponse(data);
      }

      if (pathname === '/api/zillow/find-comps') {
        const address = url.searchParams.get('address');
        if (!address) return errorResponse('address parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        const days = parseInt(url.searchParams.get('days')) || 90;
        const data = await findComps(address, days, env);
        if (data.error) return errorResponse(data.error, 400);
        return jsonResponse(data);
      }

      // Debug endpoint: raw API response so you can see exactly what ZLLW returns
      if (pathname === '/api/zillow/raw') {
        const address = url.searchParams.get('address');
        const zpid = url.searchParams.get('zpid');
        if (!address && !zpid) return errorResponse('address or zpid parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        const data = zpid ? await zillowGetByZpid(zpid, env) : await zillowGetByAddress(address, env);
        // Return the completely raw response with key analysis
        const allKeys = data ? Object.keys(data) : [];
        const arrayKeys = allKeys.filter(k => Array.isArray(data[k]));
        const objectKeys = allKeys.filter(k => data[k] && typeof data[k] === 'object' && !Array.isArray(data[k]));
        const extracted = deepExtractProperties(data, 0, 6, null);
        return jsonResponse({
          _analysis: {
            topLevelKeyCount: allKeys.length,
            topLevelKeys: allKeys,
            arrayKeys: arrayKeys,
            objectKeys: objectKeys,
            extractedPropertyCount: extracted.length,
            extractedSample: extracted.slice(0, 3).map(e => ({
              address: e.address || e.streetAddress || '?',
              zpid: e.zpid,
              price: e.price || e.soldPrice || e.lastSoldPrice,
              beds: e.bedrooms || e.beds,
              sqft: e.livingArea || e.sqft,
            })),
          },
          raw: data,
        });
      }

      // Fetch photos for a single property by zpid
      if (pathname === '/api/zillow/photos') {
        const zpid = url.searchParams.get('zpid');
        if (!zpid) return errorResponse('zpid parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        try {
          const data = await zillowGetByZpid(zpid, env);
          const photoUrls = [];
          const seenUrls = new Set();
          function extractPhotos(obj, depth) {
            if (!obj || depth > 5) return;
            if (typeof obj === 'string') {
              if ((obj.includes('zillowstatic.com') || obj.includes('.jpg') || obj.includes('.jpeg') || obj.includes('.png') || obj.includes('.webp')) && obj.startsWith('http') && !seenUrls.has(obj)) {
                seenUrls.add(obj);
                photoUrls.push(obj);
              }
              return;
            }
            if (Array.isArray(obj)) { obj.forEach(item => extractPhotos(item, depth + 1)); return; }
            if (typeof obj === 'object') {
              if (obj.mixedSources) {
                const jpegSrc = obj.mixedSources.jpeg || obj.mixedSources.webp || [];
                if (Array.isArray(jpegSrc) && jpegSrc.length > 0) {
                  const best = jpegSrc[jpegSrc.length - 1];
                  if (best && best.url && !seenUrls.has(best.url)) { seenUrls.add(best.url); photoUrls.push(best.url); }
                }
              }
              Object.values(obj).forEach(val => extractPhotos(val, depth + 1));
            }
          }
          extractPhotos(data, 0);
          const p = data?.propertyDetails || data?.data || data || {};
          return jsonResponse({
            zpid: zpid,
            address: p.address || p.streetAddress || '',
            photos: photoUrls,
            photoCount: photoUrls.length,
          });
        } catch (e) {
          return errorResponse('Failed to fetch photos: ' + e.message);
        }
      }

      if (pathname === '/api/zillow/lookup') {
        const address = url.searchParams.get('address');
        if (!address) return errorResponse('address parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        const data = await zillowFullLookup(address, env);
        return jsonResponse(data);
      }

      // Find active listing agents near a property (investor-friendly focus)
      if (pathname === '/api/zillow/listing-agents') {
        const address = url.searchParams.get('address');
        if (!address) return errorResponse('address parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        try {
          const data = await findListingAgents(address, env);
          return jsonResponse(data);
        } catch (e) {
          return errorResponse('Failed to find listing agents: ' + e.message);
        }
      }

      // ---- Rentometer API Routes ----

      if (pathname === '/api/rentometer/summary') {
        const address = url.searchParams.get('address');
        if (!address) return errorResponse('address parameter required');
        if (!env.RENTOMETER_API_KEY) return errorResponse('RENTOMETER_API_KEY not configured', 500);
        const bedrooms = url.searchParams.get('bedrooms') || '3';
        const data = await rentometerSummary(address, bedrooms, env);
        return jsonResponse(data);
      }

      // ---- Seller Photo Storage (Cloudflare KV) ----

      if (pathname === '/api/photos/upload' && request.method === 'POST') {
        // Upload a seller photo — stores base64 image in KV
        if (!env.SELLER_PHOTOS) return errorResponse('SELLER_PHOTOS KV namespace not bound', 500);
        try {
          const body = await request.json();
          const { dealId, photoData } = body;
          if (!dealId || !photoData) return errorResponse('dealId and photoData required');
          // photoData is a base64 data URL
          // Limit: 25MB per KV value, base64 images are typically 100KB-2MB
          if (photoData.length > 5 * 1024 * 1024) return errorResponse('Photo too large (max 5MB)');
          await env.SELLER_PHOTOS.put('photo:' + dealId, photoData);
          return jsonResponse({ success: true, dealId: dealId });
        } catch(e) {
          return errorResponse('Photo upload failed: ' + e.message);
        }
      }

      if (pathname === '/api/photos/get') {
        // Get a single photo by dealId
        if (!env.SELLER_PHOTOS) return errorResponse('SELLER_PHOTOS KV namespace not bound', 500);
        const dealId = url.searchParams.get('dealId');
        if (!dealId) return errorResponse('dealId parameter required');
        const photo = await env.SELLER_PHOTOS.get('photo:' + dealId);
        return jsonResponse({ dealId: dealId, photoData: photo || null });
      }

      if (pathname === '/api/photos/list') {
        // List all stored photos (returns dealIds only, not photo data)
        if (!env.SELLER_PHOTOS) return errorResponse('SELLER_PHOTOS KV namespace not bound', 500);
        const list = await env.SELLER_PHOTOS.list({ prefix: 'photo:' });
        const dealIds = list.keys.map(k => k.name.replace('photo:', ''));
        return jsonResponse({ dealIds: dealIds, count: dealIds.length });
      }

      if (pathname === '/api/photos/batch') {
        // Get multiple photos at once
        if (!env.SELLER_PHOTOS) return errorResponse('SELLER_PHOTOS KV namespace not bound', 500);
        const ids = (url.searchParams.get('dealIds') || '').split(',').filter(Boolean);
        if (ids.length === 0) return errorResponse('dealIds parameter required (comma-separated)');
        const photos = {};
        await Promise.all(ids.map(async (id) => {
          const photo = await env.SELLER_PHOTOS.get('photo:' + id);
          if (photo) photos[id] = photo;
        }));
        return jsonResponse({ photos: photos, count: Object.keys(photos).length });
      }

      if (pathname === '/api/photos/delete' && request.method === 'POST') {
        if (!env.SELLER_PHOTOS) return errorResponse('SELLER_PHOTOS KV namespace not bound', 500);
        try {
          const body = await request.json();
          if (!body.dealId) return errorResponse('dealId required');
          await env.SELLER_PHOTOS.delete('photo:' + body.dealId);
          return jsonResponse({ success: true, deleted: body.dealId });
        } catch(e) {
          return errorResponse('Delete failed: ' + e.message);
        }
      }

      if (pathname === '/health') {
        return jsonResponse({ status: 'healthy' });
      }

      // 404 for unknown routes
      return errorResponse('Endpoint not found', 404);
    } catch (error) {
      console.error('Worker error:', error);

      // Check if error is due to invalid credentials
      if (error.message.includes('401') || error.message.includes('403')) {
        return errorResponse('Authentication failed. Check API credentials.', 401);
      }

      return errorResponse('Internal server error', 500);
    }
  },
};
