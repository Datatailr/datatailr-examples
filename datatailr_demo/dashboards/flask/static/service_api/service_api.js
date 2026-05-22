// datatailr_demo/dashboards/flask/static/service_api/service_api.js
import { loadApiDocs } from './api_viewer.js'
import { loadServices } from './service_select.js'

/**
 * Process the Service API.
 * Loads the list of services and attaches an event listener to the load button.
 * @returns {void}
 */
function processServiceApi() {
  loadServices()
  document.getElementById('loadBtn').addEventListener('click', loadApiDocs)
}

/**
 * Process the Service API when the DOM is loaded.
 * @returns {void}
 */
document.addEventListener('DOMContentLoaded', processServiceApi)
