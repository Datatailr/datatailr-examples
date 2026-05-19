/**
 * Populate the service select with the list of services.
 * @param {Object[]} services - The list of services.
 */
function populateServiceSelect(services) {
  const select = document.getElementById('serviceSelect')
  if (!services.length) {
    select.innerHTML = '<option value="">No running services found</option>'
    return
  }
  select.innerHTML = services
    .map((s) => `<option value="${s.name}">${s.name}</option>`)
    .join('')
}

/**
 * Handle the error when loading the services.
 * @param {Error} error - The error that occurred.
 */
function handleLoadServicesError(error) {
  console.error(error)
  document.getElementById('serviceSelect').innerHTML =
    `<option value="">Error loading services: ${error.message}</option>`
}

/**
 * Load the services for the API viewer.
 */
function loadServices() {
  fetch(PREFIX + '/api/services')
    .then((r) => r.json())
    .then((services) => populateServiceSelect(services))
    .catch((error) => handleLoadServicesError(error))
}

export { loadServices }
