/**
 * API Client Module
 * Handles communication with FastAPI backend and loading animation
 */

// API Configuration
const API_BASE_URL = 'http://localhost:8000';
const API_ENDPOINTS = {
    searchRoutes: '/search',
    searchStream: '/search/stream',
    attractions: '/api/attractions'
};

// Loading animation instance
let loadingConsole = null;

/**
 * Show loading overlay with animation
 * @param {string[]} iataCodes - Array of IATA codes
 */
function showLoadingAnimation(iataCodes) {
    const overlay = document.getElementById('loading-overlay');
    overlay.classList.add('active');

    if (iataCodes.length < 1) {
        console.warn('No valid airports for animation');
        return null;
    }

    // First city is origin, rest are destinations
    const origin = iataCodes[0];
    const destinations = iataCodes.slice(1);

    // Initialize the generative console
    loadingConsole = new window.GenerativeConsole('loading-map');
    loadingConsole.init({
        origin: origin,
        destinations: destinations,
        duration: document.getElementById('summary-duration')?.textContent || '5 days'
    });

    return loadingConsole;
}

/**
 * Hide loading overlay
 */
function hideLoadingAnimation() {
    const overlay = document.getElementById('loading-overlay');
    overlay.classList.remove('active');

    if (loadingConsole) {
        loadingConsole.destroy();
        loadingConsole = null;
    }
}

/**
 * Fetch tourist attractions for cities (non-blocking)
 * @param {string[]} cities - Array of city names
 * @returns {Promise<Object>} Map of city -> attractions array
 */
async function fetchAttractions(cities) {
    if (!cities || cities.length === 0) return {};

    try {
        const citiesParam = cities.join(',');
        const response = await fetch(
            `${API_BASE_URL}${API_ENDPOINTS.attractions}?cities=${encodeURIComponent(citiesParam)}`
        );

        if (!response.ok) {
            console.warn('Attractions fetch failed:', response.status);
            return {};
        }

        return await response.json();
    } catch (error) {
        console.warn('Failed to fetch attractions:', error);
        return {}; // Fail gracefully - animation works without attractions
    }
}

/**
 * Update loading UI with stage info
 * @param {Object} stageInfo - Stage information
 */
function updateLoadingUI(stageInfo) {
    const titleEl = document.getElementById('loading-title');
    const messageEl = document.getElementById('loading-message');
    const iconEl = document.getElementById('loading-icon');
    const progressEl = document.getElementById('loading-progress');
    const percentEl = document.getElementById('loading-percent');

    if (titleEl) titleEl.textContent = stageInfo.title;
    if (messageEl) messageEl.textContent = stageInfo.message;
    if (iconEl) {
        iconEl.className = `fa-solid ${stageInfo.icon} text-gold-400`;
    }
    if (progressEl) progressEl.style.width = `${stageInfo.progress}%`;
    if (percentEl) percentEl.textContent = stageInfo.progress;
}

/**
 * Search routes with SSE streaming (includes validation)
 * @param {Object} requestData - Search request data
 * @param {Function} onStageChange - Callback for stage changes ('routing', 'validating')
 * @returns {Promise<Array>} Final validated routes
 */
async function searchRoutesStream(requestData, onStageChange) {
    const response = await fetch(`${API_BASE_URL}${API_ENDPOINTS.searchStream}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestData)
    });

    if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    // Helper function to process SSE event block
    function processEventBlock(eventBlock, onStageChange) {
        if (!eventBlock.trim()) return null;

        // Parse SSE format: "event: xxx\ndata: yyy"
        const lines = eventBlock.split('\n');
        let eventType = null;
        let eventData = null;

        for (const line of lines) {
            if (line.startsWith('event:')) {
                eventType = line.slice(6).trim();
            } else if (line.startsWith('data:')) {
                eventData = line.slice(5).trim();
            }
        }

        console.log('SSE event:', eventType, eventData);

        if (eventType === 'stage' && onStageChange) {
            onStageChange(eventData); // 'routing' or 'validating'
        }

        if (eventType === 'complete' && eventData) {
            const result = JSON.parse(eventData);
            console.log('SSE complete, routes:', result.routes);
            return result.routes || [];
        }

        return null;
    }

    while (true) {
        const { done, value } = await reader.read();

        if (done) {
            // Stream ended - process any remaining buffer
            console.log('SSE stream ended, remaining buffer:', buffer);
            if (buffer.trim()) {
                const result = processEventBlock(buffer, onStageChange);
                if (result !== null) return result;
            }
            break;
        }

        buffer += decoder.decode(value, { stream: true });
        const events = buffer.split('\n\n');
        buffer = events.pop(); // Keep incomplete chunk

        for (const eventBlock of events) {
            const result = processEventBlock(eventBlock, onStageChange);
            if (result !== null) return result;
        }
    }

    return []; // No routes found
}

/**
 * Handle form submission - EVENT-DRIVEN animation
 */
function handleFormSubmit(event) {
    event.preventDefault();

    const origin = window.CitySelector.getOriginIata();
    const cities = window.CitySelector.getSelectedCities();
    const destinations = window.CitySelector.getSelectedIatas();
    const departureDate = document.getElementById('start-date').value;
    const returnDate = document.getElementById('end-date').value;

    // Validation
    if (!origin) {
        alert('Please select a starting city');
        return;
    }

    if (destinations.length < 1) {
        alert('Please add at least one destination');
        return;
    }

    if (!departureDate || !returnDate) {
        alert('Please select both departure and return dates');
        return;
    }

    // Get origin city name for display
    const originInput = document.getElementById('origin-input');
    const originCity = originInput ? originInput.value : origin;

    // Get optional min stay hours
    const minStayInput = document.getElementById('min-stay');
    const minStayHours = minStayInput && minStayInput.value ? parseFloat(minStayInput.value) : null;

    // Store search data for results page
    const searchData = {
        origin: origin,
        origin_city: originCity,
        cities: cities,
        destinations: destinations,
        departure_date: departureDate,
        return_date: returnDate,
        min_stay_hours: minStayHours,
    };
    sessionStorage.setItem('routeSearchData', JSON.stringify(searchData));

    // Show loading animation (origin + destinations for map)
    const allIatas = [origin, ...destinations];
    const animationConsole = showLoadingAnimation(allIatas);

    function redirect() {
        hideLoadingAnimation();
        window.location.href = 'results.html';
    }

    // Request data for streaming API
    const requestData = {
        origin: origin,
        destinations: destinations,
        departure_date: `${departureDate}T00:00:00`,
        return_date: returnDate ? `${returnDate}T00:00:00` : null,
        min_stay_hours: minStayHours,
    };

    if (animationConsole) {
        // Start animation in EVENT-DRIVEN mode
        // Animation will wait at stage boundaries for backend signals
        animationConsole.startEventDriven(
            (stageInfo) => updateLoadingUI(stageInfo)
        );

        // Fetch attractions in parallel (non-blocking)
        // This enriches the loading experience with tourist info
        fetchAttractions(cities).then(attractions => {
            console.log('Attractions loaded:', Object.keys(attractions));
            animationConsole.setAttractions(attractions);
            animationConsole.startAttractionCycle(2500);
        });

        // API call with stage signals that DRIVE the animation
        searchRoutesStream(requestData, (stage) => {
            console.log('Backend stage:', stage);

            if (stage === 'routing') {
                // Backend started routing - animation can proceed through stages 0-1
                // (it's already doing this)
            }

            if (stage === 'validating') {
                // Backend finished routing, started validation
                // Tell animation to advance to stage 2 (Optimizing Route)
                animationConsole.advanceToStage(2);
            }
        })
        .then(routes => {
            console.log('Got routes from API:', routes);
            console.log('Routes type:', typeof routes);
            console.log('Routes is array:', Array.isArray(routes));
            console.log('Routes length:', routes ? routes.length : 'null');

            // Store results in sessionStorage
            const jsonString = JSON.stringify(routes);
            console.log('Storing JSON (first 500 chars):', jsonString.substring(0, 500));
            sessionStorage.setItem('routeResults', jsonString);

            // Verify storage worked
            const stored = sessionStorage.getItem('routeResults');
            console.log('Verified storage (first 500 chars):', stored ? stored.substring(0, 500) : 'null');

            // Complete animation (attractions keep showing until redirect)
            animationConsole.completeAndFinish(() => {
                console.log('Animation complete, redirecting...');
                animationConsole.stopAttractionCycle();
                redirect();
            });
        })
        .catch(error => {
            console.error('Search failed:', error);
            animationConsole.stopAttractionCycle();
            hideLoadingAnimation();
            alert('Search failed. Please try again.');
        });
    } else {
        // Fallback if animation can't run - just call API and redirect
        searchRoutesStream(requestData, () => {})
            .then(routes => {
                sessionStorage.setItem('routeResults', JSON.stringify(routes));
                redirect();
            })
            .catch(error => {
                console.error('Search failed:', error);
                alert('Search failed. Please try again.');
            });
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', async () => {
    // Load airport data for the animation
    await window.AirportIndex.load();
    console.log('Airport index loaded');

    // Attach form submission handler
    const form = document.getElementById('trip-form');
    if (form) {
        form.addEventListener('submit', handleFormSubmit);
    }
});

// Export for external use
window.ApiClient = {
    searchRoutesStream,
    showLoadingAnimation,
    hideLoadingAnimation,
    fetchAttractions,
    API_BASE_URL,
    API_ENDPOINTS
};
