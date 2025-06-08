const API_BASE_URL = 'http://localhost:5000/api/v1';

// Función para manejar errores de fetch
function handleFetchError(error) {
    console.error('Error:', error);
    return `
        <div class="alert alert-danger">
            <i class="bi bi-exclamation-triangle-fill"></i>
            Error al cargar los datos. Por favor, intente nuevamente.
        </div>`;
}

// Función para formatear el timestamp
function formatTimestamp(bucket) {
    if (!bucket) return '';
    const day = bucket.substring(6, 8);
    const month = bucket.substring(4, 6);
    const year = bucket.substring(0, 4);
    const hour = bucket.substring(8, 10);
    return `${day}/${month}/${year} ${hour}:00`;
}

// Verificar estado de la API
async function checkApiStatus() {
    const statusElement = document.getElementById('api-status');
    statusElement.classList.add('loading');
    
    try {
        const response = await fetch(`${API_BASE_URL}/status`);
        const data = await response.json();
        
        if (data.cassandra_connected) {
            statusElement.innerHTML = `
                <div class="alert alert-success">
                    <i class="bi bi-check-circle-fill"></i> 
                    <strong>Sistema Operativo</strong>
                    <br>
                    <small>Conexión establecida con Cassandra</small>
                </div>`;
        } else {
            statusElement.innerHTML = `
                <div class="alert alert-warning">
                    <i class="bi bi-exclamation-triangle-fill"></i>
                    <strong>Advertencia</strong>
                    <br>
                    <small>API operativa pero sin conexión a Cassandra</small>
                </div>`;
        }
    } catch (error) {
        statusElement.innerHTML = `
            <div class="alert alert-danger">
                <i class="bi bi-x-circle-fill"></i>
                <strong>Error</strong>
                <br>
                <small>No se puede establecer conexión con la API</small>
            </div>`;
    }
    
    statusElement.classList.remove('loading');
}

// Cargar clientes recientes
async function loadRecentCustomers() {
    const container = document.getElementById('recent-customers');
    container.classList.add('loading');
    
    try {
        const response = await fetch(`${API_BASE_URL}/customers/global_recent?limit=5`);
        const data = await response.json();
        
        if (data.global_recent_customers && data.global_recent_customers.length > 0) {
            const customersList = data.global_recent_customers.map(customer => `
                <div class="border-bottom py-2">
                    <div class="d-flex align-items-center">
                        <div class="bg-light rounded-circle p-2 me-2">
                            <i class="bi bi-person"></i>
                        </div>
                        <div>
                            <h6 class="mb-0">${customer.first_name} ${customer.last_name}</h6>
                            <small class="text-muted">
                                <i class="bi bi-envelope"></i> ${customer.email_address}<br>
                                <i class="bi bi-geo-alt"></i> ${customer.city}<br>
                                <i class="bi bi-clock"></i> ${new Date(customer.registration_timestamp).toLocaleString()}
                            </small>
                        </div>
                    </div>
                </div>
            `).join('');
            
            container.innerHTML = customersList;
        } else {
            container.innerHTML = `
                <div class="text-center text-muted">
                    <i class="bi bi-people" style="font-size: 2rem;"></i>
                    <p>No se encontraron clientes recientes</p>
                </div>`;
        }
    } catch (error) {
        container.innerHTML = handleFetchError(error);
    }
    
    container.classList.remove('loading');
}

// Cargar productos nuevos
async function loadNewProducts() {
    const container = document.getElementById('new-products');
    const period = document.getElementById('period-select').value;
    container.classList.add('loading');
    
    try {
        const response = await fetch(`${API_BASE_URL}/products/new_count?period=${period}`);
        const data = await response.json();
        
        if (data && data.new_products_count !== undefined) {
            container.innerHTML = `
                <div class="d-flex align-items-center">
                    <div class="stats-value me-2">${data.new_products_count}</div>
                    <div class="text-muted me-3">productos</div>
                    <div class="badge bg-success">
                        <i class="bi bi-clock"></i> ${formatTimestamp(data.time_bucket)}
                    </div>
                </div>`;
        } else {
            container.innerHTML = `
                <div class="alert alert-warning">
                    <i class="bi bi-exclamation-triangle-fill"></i>
                    No hay datos disponibles
                </div>`;
        }
    } catch (error) {
        container.innerHTML = handleFetchError(error);
    }
    
    container.classList.remove('loading');
}

// Cargar productos por categoría
async function loadProductsByCategory() {
    const container = document.getElementById('products-by-category');
    const categoryKey = document.getElementById('category-select').value;
    container.classList.add('loading');
    
    try {
        const response = await fetch(`${API_BASE_URL}/products/recent_by_category/${categoryKey}`);
        const data = await response.json();
        
        if (data.recent_products && data.recent_products.length > 0) {
            const productsList = data.recent_products.map(product => `
                <div class="product-card border-bottom py-2">
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <h6 class="mb-0">${product.english_product_name}</h6>
                            <small class="text-muted">
                                <i class="bi bi-palette"></i> ${product.color}
                            </small>
                        </div>
                        <small class="text-muted">
                            ${new Date(product.addition_timestamp).toLocaleString()}
                        </small>
                    </div>
                </div>
            `).join('');
            
            container.innerHTML = `
                <div class="mb-3">
                    <span class="badge bg-warning text-dark">
                        ${data.category_name}
                    </span>
                </div>
                ${productsList}`;
        } else {
            container.innerHTML = `
                <div class="text-center text-muted">
                    <i class="bi bi-bicycle" style="font-size: 2rem;"></i>
                    <p>No hay productos recientes en esta categoría</p>
                </div>`;
        }
    } catch (error) {
        container.innerHTML = handleFetchError(error);
    }
    
    container.classList.remove('loading');
}

// Cargar distribución geográfica
async function loadGeoDistribution() {
    const container = document.getElementById('geo-distribution');
    const country = document.getElementById('country-select').value;
    container.classList.add('loading');
    
    try {
        const response = await fetch(`${API_BASE_URL}/customers/geo_distribution_hourly_by_country/${country}`);
        const data = await response.json();
        
        if (data.distribution_by_city && data.distribution_by_city.length > 0) {
            const citiesList = data.distribution_by_city.map(city => `
                <div class="col-md-3 col-sm-6 mb-3">
                    <div class="card h-100 product-card">
                        <div class="card-body text-center">
                            <h6 class="card-title text-primary">${city.city}</h6>
                            <div class="stats-value text-success">${city.new_customers_count}</div>
                            <small class="text-muted">nuevos clientes</small>
                        </div>
                    </div>
                </div>
            `).join('');
            
            container.innerHTML = `
                <div class="text-center mb-4">
                    <h4>Total de nuevos clientes en ${country}:</h4>
                    <div class="stats-value text-primary">${data.total_new_customers_in_hour_for_country}</div>
                    <div class="badge bg-info">
                        <i class="bi bi-clock"></i> ${formatTimestamp(data.hour_bucket)}
                    </div>
                </div>
                <div class="row">
                    ${citiesList}
                </div>`;
        } else {
            container.innerHTML = `
                <div class="text-center text-muted">
                    <i class="bi bi-globe" style="font-size: 2rem;"></i>
                    <p>No hay datos de distribución geográfica disponibles</p>
                </div>`;
        }
    } catch (error) {
        container.innerHTML = handleFetchError(error);
    }
    
    container.classList.remove('loading');
}

// Inicializar selectores
async function initializeSelectors() {
    // Inicializar selector de categorías
    const categorySelect = document.getElementById('category-select');
    categorySelect.innerHTML = `
        <option value="1">Mountain Bikes</option>
        <option value="2">Road Bikes</option>
        <option value="3">Touring Bikes</option>
    `;

    // Inicializar selector de países
    const countrySelect = document.getElementById('country-select');
    countrySelect.innerHTML = `
        <option value="United States">Estados Unidos</option>
        <option value="Canada">Canadá</option>
        <option value="Mexico">México</option>
    `;
}

// Inicializar dashboard
async function initializeDashboard() {
    await initializeSelectors();
    checkApiStatus();
    loadRecentCustomers();
    loadNewProducts();
    loadProductsByCategory();
    loadGeoDistribution();

    // Actualizar datos cada 30 segundos
    setInterval(checkApiStatus, 30000);
    setInterval(loadRecentCustomers, 30000);
    setInterval(loadNewProducts, 30000);
    setInterval(loadProductsByCategory, 30000);
    setInterval(loadGeoDistribution, 30000);
}
