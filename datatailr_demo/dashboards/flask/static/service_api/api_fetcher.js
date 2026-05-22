// datatailr_demo/dashboards/flask/static/service_api/api_fetcher.js

/**
 * Fetch a URL with credentials included.
 * @param {string} url - The URL to fetch.
 * @param {Object} options - Additional fetch options.
 * @returns {Promise<Response>} The fetch response.
 */
async function fetchWithCredentials(url, options = {}) {
  return fetch(url, { credentials: 'include', ...options })
}

/**
 * Fetch the health status of a service directly from the browser.
 * @param {string} baseUrl - The base URL of the service.
 * @returns {Promise<Object>} The health status.
 */
async function fetchHealthStatus(baseUrl) {
  const res = { status: '', code: null }
  try {
    const url = `${baseUrl}/health`
    const resp = await fetchWithCredentials(url)
    res.status = resp.ok ? 'healthy' : 'unhealthy'
    res.code = resp.status
  } catch (e) {
    res.status = 'unreachable'
    res.code = e.code
  }
  return res
}

/**
 * Fetch the OpenAPI spec of a service directly from the browser.
 * @param {string} baseUrl - The base URL of the service.
 * @returns {Promise<Object|null>} The OpenAPI spec or null.
 */
async function fetchOpenApiSpec(baseUrl) {
  try {
    const url = `${baseUrl}/openapi.json`
    const headers = { Accept: 'application/json,*/*' }
    const resp = await fetchWithCredentials(url, { headers })
    if (!resp.ok) return null
    const spec = await resp.json()
    spec.servers = [{ url: `${baseUrl.replace(/\/$/, '')}/` }]
    return spec
  } catch (e) {
    return null
  }
}

export { fetchHealthStatus, fetchOpenApiSpec }
