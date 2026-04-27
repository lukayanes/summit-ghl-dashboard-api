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
      address: comp.address || comp.streetAddress || comp.formattedAddress || '',
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
 * Find comps for a subject property
 * 1. Get subject details from Zillow
 * 2. Search recently sold in the same zip
 * 3. Score & rank each sold property
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

  // Extract subject photos from various Zillow response shapes
  const subjectPhotos = [];
  const photoSources = p.photos || p.responsivePhotos || p.hugePhotos || p.hiResPhotos || [];
  if (Array.isArray(photoSources)) {
    photoSources.forEach(photo => {
      if (typeof photo === 'string') { subjectPhotos.push(photo); }
      else if (photo?.url) { subjectPhotos.push(photo.url); }
      else if (photo?.mixedSources?.jpeg) {
        // Zillow responsive format — pick the largest
        const jpegs = photo.mixedSources.jpeg;
        const largest = jpegs.reduce((best, cur) => ((cur.width || 0) > (best.width || 0) ? cur : best), jpegs[0]);
        if (largest?.url) subjectPhotos.push(largest.url);
      }
    });
  }

  // Extract last sale price and date from various Zillow fields
  let lastSalePrice = p.lastSoldPrice || p.lastSalePrice || p.soldPrice || p.salePrice || null;
  let lastSaleDate = p.dateSold || p.lastSoldDate || p.dateSoldString || null;

  // Check priceHistory for the most recent sale event
  if (Array.isArray(p.priceHistory) && p.priceHistory.length > 0) {
    const saleEvents = p.priceHistory.filter(e => e.event && (e.event.toLowerCase() === 'sold' || e.event.toLowerCase().includes('sold')));
    if (saleEvents.length > 0) {
      // Sort by date descending
      saleEvents.sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
      if (!lastSalePrice && saleEvents[0].price) lastSalePrice = saleEvents[0].price;
      if (!lastSaleDate && saleEvents[0].date) lastSaleDate = saleEvents[0].date;
    }
  }

  // Also check taxHistory as a fallback
  if (!lastSalePrice && Array.isArray(p.taxHistory) && p.taxHistory.length > 0) {
    const withValue = p.taxHistory.filter(t => t.taxPaid || t.value);
    // taxHistory doesn't always have sale prices, but can give clues
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

  // Step 2: Deep-extract all property-like objects from the response
  // The ZLLW API embeds comps, nearby sales, similar homes in the property data
  const extractedProperties = deepExtractProperties(propertyData, 0, 6, subject.zpid);

  // Step 2b: If we have zpids from extracted properties that lack full data, try fetching them
  // Also try fetching comps by zpid if the property response includes a comps list with just zpids
  const zpidsToFetch = [];
  const fullDataProps = [];

  extractedProperties.forEach(ep => {
    const hasFullData = (ep.livingArea || ep.sqft || ep.bedrooms || ep.beds) && (ep.price || ep.soldPrice || ep.lastSoldPrice);
    if (hasFullData) {
      fullDataProps.push(ep);
    } else if (ep.zpid) {
      zpidsToFetch.push(ep.zpid);
    }
  });

  // Fetch up to 10 individual zpids for more detail (parallel, with error tolerance)
  if (zpidsToFetch.length > 0) {
    const toFetch = zpidsToFetch.slice(0, 10);
    const fetched = await Promise.allSettled(
      toFetch.map(zpid => zillowGetByZpid(zpid, env))
    );
    fetched.forEach(result => {
      if (result.status === 'fulfilled' && result.value) {
        const fd = result.value?.propertyDetails || result.value?.data || result.value || {};
        if (fd.zpid || fd.address || fd.streetAddress) {
          fullDataProps.push(fd);
        }
      }
    });
  }

  // Step 3: Filter by days and score each
  const cutoffDate = new Date(Date.now() - (daysBack || 90) * 24 * 60 * 60 * 1000);
  const scoredComps = [];
  const seenZpids = new Set();

  fullDataProps.forEach(comp => {
    if (!comp || typeof comp !== 'object') return;
    // Skip the subject property itself
    if (comp.zpid && comp.zpid === subject.zpid) return;
    // Skip dupes
    if (comp.zpid && seenZpids.has(comp.zpid)) return;
    if (comp.zpid) seenZpids.add(comp.zpid);

    // Date filter — if sold date is available, check it
    const soldDateStr = comp.dateSold || comp.lastSoldDate || comp.datePosted || comp.dateSoldString || null;
    if (soldDateStr) {
      const soldDate = new Date(soldDateStr);
      if (!isNaN(soldDate.getTime()) && soldDate < cutoffDate) return;
    }

    // Filter out rental listings
    const compStatus = (comp.homeStatus || comp.statusType || comp.listingStatus || comp.status || '').toString().toLowerCase();
    if (compStatus.includes('for_rent') || compStatus.includes('rental') || compStatus === 'rent') return;

    // Must have a SALE price — prioritize sold-specific fields over generic 'price'
    const price = comp.soldPrice || comp.lastSoldPrice || comp.lastSalePrice || comp.salePrice || comp.price || 0;
    if (price <= 0) return;

    // Skip if price looks like a monthly rent (under $10k and no sold date)
    if (price < 10000 && !soldDateStr) return;

    // Calculate distance if we have coords
    let distance = null;
    if (subject.latitude && subject.longitude && (comp.latitude && comp.longitude)) {
      distance = haversineDistance(subject.latitude, subject.longitude, comp.latitude, comp.longitude);
      // Skip if > 5 miles
      if (distance > 5) return;
    }

    comp._distance = distance;
    const scoring = scoreComp(subject, comp);

    scoredComps.push({
      address: comp.address || comp.streetAddress || comp.formattedAddress || '',
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
      // Price source tracking — which field the price came from
      _priceSource: comp.soldPrice ? 'soldPrice' : comp.lastSoldPrice ? 'lastSoldPrice' : comp.lastSalePrice ? 'lastSalePrice' : comp.salePrice ? 'salePrice' : 'price',
      // Scoring
      matchPct: scoring.matchPct,
      grade: scoring.grade,
      scoreBreakdown: scoring,
    });
  });

  // Sort by match percentage (best first)
  scoredComps.sort((a, b) => b.matchPct - a.matchPct);

  // Calculate ARV from top comps (grade A or B, or top 5)
  const topComps = scoredComps.filter(c => c.grade === 'A' || c.grade === 'B').slice(0, 5);
  const arvComps = topComps.length >= 3 ? topComps : scoredComps.slice(0, 5);
  const arvPrices = arvComps.map(c => c.price).filter(p => p > 0);
  const avgARV = arvPrices.length > 0 ? Math.round(arvPrices.reduce((a, b) => a + b, 0) / arvPrices.length) : null;
  const medianARV = arvPrices.length > 0 ? arvPrices.sort((a, b) => a - b)[Math.floor(arvPrices.length / 2)] : null;

  return {
    subject: subject,
    comps: scoredComps,
    compCount: scoredComps.length,
    totalExtracted: extractedProperties.length,
    zpidsFetched: zpidsToFetch.length,
    fullDataCount: fullDataProps.length,
    daysBack: daysBack || 90,
    suggestedARV: avgARV,
    medianARV: medianARV,
    arvCompsUsed: arvComps.length,
    zestimate: subject.zestimate,
    generatedAt: new Date().toISOString(),
    _debug: {
      topLevelKeys: Object.keys(propertyData || {}),
      extractedRaw: extractedProperties.length,
      zpidsFetchedCount: Math.min(zpidsToFetch.length, 10),
      fullDataAfterFetch: fullDataProps.length,
      filteredOut: fullDataProps.length - scoredComps.length,
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

    // Only allow GET requests
    if (request.method !== 'GET') {
      return errorResponse('Method not allowed', 405);
    }

    const url = new URL(request.url);
    const pathname = url.pathname;

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

      // ---- Rentometer API Routes ----

      if (pathname === '/api/rentometer/summary') {
        const address = url.searchParams.get('address');
        if (!address) return errorResponse('address parameter required');
        if (!env.RENTOMETER_API_KEY) return errorResponse('RENTOMETER_API_KEY not configured', 500);
        const bedrooms = url.searchParams.get('bedrooms') || '3';
        const data = await rentometerSummary(address, bedrooms, env);
        return jsonResponse(data);
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
