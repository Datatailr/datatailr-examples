/**
 * Render a health badge for a service.
 * @param {Object} health - The health status of the service.
 * @returns {string} The HTML for the health badge.
 */
function renderHealthBadge(health) {
  const color = health.status === 'healthy' ? '#00b894' : '#e17055'
  return `
      <div style="margin-bottom: 1rem;">
        <span style="background: ${color}; color: white; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.85rem;">
          ● ${health.status}
        </span>
      </div>`
}

/**
 * Render the Swagger UI for a service.
 * @param {Object} spec - The OpenAPI specification for the service.
 */
function renderSwaggerUI(spec) {
  SwaggerUIBundle({
    spec: spec,
    domNode: document.getElementById('swagger-ui'),
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIBundle.SwaggerUIStandalonePreset,
    ],
    layout: 'BaseLayout',
  })
}

/**
 * Render an error message in the viewer.
 * @param {string} message - The error message.
 * @param {HTMLElement} viewer - The element to display the error.
 */
function renderError(message, viewer) {
  viewer.innerHTML = `<p style="color: red; padding: 1rem;">Error: ${message}</p>`
}

/**
 * Render the API viewer for a service.
 * @param {Object} data - The data containing the health status and OpenAPI specification.
 */
function renderApiViewer(data) {
  const viewer = document.getElementById('api-viewer')
  const healthBadge = renderHealthBadge(data.health)

  if (!data.spec) {
    viewer.innerHTML =
      healthBadge +
      '<p style="color: #888; padding: 1rem;">No OpenAPI spec available for this service.</p>'
    return
  }

  viewer.innerHTML = healthBadge + '<div id="swagger-ui"></div>'
  renderSwaggerUI(data.spec)
}

/**
 * Handle the error when loading the API docs.
 * @param {Error} error - The error that occurred.
 * @param {HTMLElement} viewer - The element to display the error.
 */
function handleLoadApiDocsError(error, viewer) {
  console.error(error)
  renderError(error.message, viewer)
}

/**
 * Process the API docs.
 * @param {Object} data - The data containing the health status and OpenAPI specification.
 * @param {HTMLElement} viewer - The element to display the data.
 */
function processApiDocs(data, viewer) {
  if (data.error) {
    renderError(data.error, viewer)
    return
  }
  renderApiViewer(data)
}

/**
 * Load the API docs for a service.
 */
function loadApiDocs() {
  const name = document.getElementById('serviceSelect').value
  if (!name) return

  const viewer = document.getElementById('api-viewer')
  viewer.innerHTML =
    '<p style="color: #888; text-align: center; padding: 2rem;">Loading...</p>'

  fetch(PREFIX + '/api/service-openapi?name=' + encodeURIComponent(name))
    .then((r) => r.json())
    .then((data) => processApiDocs(data, viewer))
    .catch((error) => handleLoadApiDocsError(error, viewer))
}

export { loadApiDocs }
