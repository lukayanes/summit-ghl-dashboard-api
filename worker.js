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
 * Search properties in an area (ZLLW Working API - Search)
 * Used for finding comps near a location
 */
async function zillowSearch(location, statusType, env) {
  const cacheKey = 'search_' + location + '_' + statusType;
  const cached = getZillowCache(cacheKey);
  if (cached) return cached;

  const encoded = encodeURIComponent(location);
  const data = await zillowRequest(`/pro/search?location=${encoded}&status=${statusType || 'recentlySold'}`, env);
  setZillowCache(cacheKey, data);
  return data;
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

  // Step 2: Search for recently sold comps in the same zip/area
  let compData = null;
  const zipMatch = address.match(/\d{5}/);
  const zip = zipMatch ? zipMatch[0] : (p.zipcode || p.zip || null);
  const city = p.city || '';
  const state = p.state || '';
  const searchLocation = zip || (city && state ? city + ', ' + state : '');

  if (searchLocation) {
    try {
      compData = await zillowSearch(searchLocation, 'recentlySold', env);
    } catch (e) {
      console.error('Comp search error:', e.message);
    }
  }

  // Extract photos from the property data
  // ZLLW API often includes photos in the property response
  const photos = p.photos || p.images || p.responsivePhotos || p.hugePhotos || p.hiResImageLink || [];
  const photoUrls = [];
  if (Array.isArray(photos)) {
    photos.forEach(photo => {
      if (typeof photo === 'string') {
        photoUrls.push(photo);
      } else if (photo.url) {
        photoUrls.push(photo.url);
      } else if (photo.mixedSources) {
        const jpegSources = photo.mixedSources.jpeg || [];
        if (jpegSources.length > 0) {
          // Get the largest available
          const best = jpegSources[jpegSources.length - 1];
          photoUrls.push(best.url);
        }
      } else if (photo.caption !== undefined && photo.url === undefined) {
        // Skip caption-only entries
      }
    });
  } else if (typeof photos === 'string') {
    photoUrls.push(photos);
  }

  // Extract comps from search results
  const comps = [];
  const compResults = compData?.results || compData?.props || compData?.searchResults || [];
  if (Array.isArray(compResults)) {
    compResults.forEach(comp => {
      // Don't include the subject property in comps
      if (comp.zpid && comp.zpid === zpid) return;
      comps.push({
        address: comp.address || comp.streetAddress || '',
        price: comp.price || comp.soldPrice || comp.lastSoldPrice || 0,
        bedrooms: comp.bedrooms || comp.beds || null,
        bathrooms: comp.bathrooms || comp.baths || null,
        livingArea: comp.livingArea || comp.sqft || null,
        lotSize: comp.lotSize || comp.lotAreaValue || null,
        yearBuilt: comp.yearBuilt || null,
        soldDate: comp.dateSold || comp.lastSoldDate || null,
        distance: comp.distance || null,
        zpid: comp.zpid || null,
      });
    });
  }

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
    comps: comps,
    rawProperty: propertyData,
    rawComps: compData,
  };

  return summary;
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

      if (pathname === '/api/zillow/search') {
        const location = url.searchParams.get('location') || url.searchParams.get('address');
        const status = url.searchParams.get('status') || 'recentlySold';
        if (!location) return errorResponse('location parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        const data = await zillowSearch(location, status, env);
        return jsonResponse(data);
      }

      if (pathname === '/api/zillow/property') {
        const address = url.searchParams.get('address');
        const zpid = url.searchParams.get('zpid');
        if (!address && !zpid) return errorResponse('address or zpid parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        const data = zpid ? await zillowGetByZpid(zpid, env) : await zillowGetByAddress(address, env);
        return jsonResponse(data);
      }

      if (pathname === '/api/zillow/lookup') {
        const address = url.searchParams.get('address');
        if (!address) return errorResponse('address parameter required');
        if (!env.ZILLOW_API_KEY) return errorResponse('ZILLOW_API_KEY not configured', 500);
        const data = await zillowFullLookup(address, env);
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
