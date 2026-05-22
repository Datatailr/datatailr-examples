// datatailr_demo/dashboards/flask/static/service_api/api_viewer.js
import { fetchHealthStatus, fetchOpenApiSpec } from './api_fetcher.js'
import { loading, renderApiViewer } from './api_renderer.js'

/**
 * Get the Datatailr environment from the page template.
 * @returns {string} The current Datatailr environment.
 */
function getEnv() {
  const envElement = document.getElementById('service-api-env')
  return JSON.parse(envElement.textContent)
}

/**
 * Build the external base URL for a service.
 * @param {string} serviceName - The name of the service.
 * @returns {string} The external base URL.
 */
function buildServiceUrl(serviceName) {
  return `${window.location.origin}/job/${getEnv()}/${serviceName}`
}

/**
 * Load the API docs for a service.
 */
async function loadApiDocs() {
  const name = document.getElementById('serviceSelect').value
  if (!name) return

  const viewer = document.getElementById('api-viewer')
  viewer.innerHTML = loading()

  const baseUrl = buildServiceUrl(name)
  const [health, spec] = await Promise.all([
    fetchHealthStatus(baseUrl),
    fetchOpenApiSpec(baseUrl),
  ])

  renderApiViewer({ health, spec })
}

export { loadApiDocs }
