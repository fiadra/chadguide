/**
 * City Selector Module
 * Handles city selection with autocomplete from available airports
 */

// DOM Elements - Destinations
const cityInput = document.getElementById('city-input');
const addCityBtn = document.getElementById('add-city-btn');
const selectedCities = document.getElementById('selected-cities');
const cityCount = document.getElementById('city-count');
const emptyCitiesHint = document.getElementById('empty-cities-hint');
const summaryCities = document.getElementById('summary-cities');
const startDateInput = document.getElementById('start-date');
const endDateInput = document.getElementById('end-date');
const summaryDuration = document.getElementById('summary-duration');

// DOM Elements - Origin
const originInput = document.getElementById('origin-input');
const originIataInput = document.getElementById('origin-iata');
const originSearchIcon = document.getElementById('origin-search-icon');
const originCheckIcon = document.getElementById('origin-check-icon');

// City tag color rotation
const cityColors = [
    'bg-navy-800 text-cream-100',
    'bg-terracotta-500 text-white',
    'bg-sage-500 text-white',
    'bg-gold-500 text-navy-900',
    'bg-navy-600 text-cream-100'
];

// Autocomplete state - Destinations
let autocompleteDropdown = null;
let selectedSuggestionIndex = -1;
let currentSuggestions = [];

// Autocomplete state - Origin
let originAutocompleteDropdown = null;
let originSelectedIndex = -1;
let originSuggestions = [];

/**
 * Initialize autocomplete dropdown
 */
function initAutocomplete() {
    // Create dropdown element
    autocompleteDropdown = document.createElement('div');
    autocompleteDropdown.id = 'city-autocomplete';
    autocompleteDropdown.className = 'absolute top-full left-0 right-0 mt-1 bg-white border-2 border-cream-300 rounded-xl shadow-lg z-50 hidden max-h-64 overflow-y-auto';

    // Insert after input container
    const inputContainer = cityInput.parentElement;
    inputContainer.style.position = 'relative';
    inputContainer.appendChild(autocompleteDropdown);

    // Input event listeners
    cityInput.addEventListener('input', handleInputChange);
    cityInput.addEventListener('keydown', handleKeyDown);
    cityInput.addEventListener('focus', handleInputFocus);
    cityInput.addEventListener('blur', handleInputBlur);

    // Disable direct add button - only autocomplete allowed
    addCityBtn.style.display = 'none';
}

/**
 * Initialize origin autocomplete
 */
function initOriginAutocomplete() {
    // Create dropdown element
    originAutocompleteDropdown = document.createElement('div');
    originAutocompleteDropdown.id = 'origin-autocomplete';
    originAutocompleteDropdown.className = 'absolute top-full left-0 right-0 mt-1 bg-white border-2 border-cream-300 rounded-xl shadow-lg z-50 hidden max-h-64 overflow-y-auto';

    // Insert after input container
    const inputContainer = originInput.parentElement;
    inputContainer.style.position = 'relative';
    inputContainer.appendChild(originAutocompleteDropdown);

    // Input event listeners
    originInput.addEventListener('input', handleOriginInputChange);
    originInput.addEventListener('keydown', handleOriginKeyDown);
    originInput.addEventListener('focus', handleOriginFocus);
    originInput.addEventListener('blur', handleOriginBlur);
}

/**
 * Handle origin input changes
 */
function handleOriginInputChange(e) {
    const query = e.target.value.trim();

    // Clear selection when typing
    originIataInput.value = '';
    originSearchIcon.classList.remove('hidden');
    originCheckIcon.classList.add('hidden');

    if (query.length < 1) {
        hideOriginAutocomplete();
        return;
    }

    // Search airports
    const results = window.AirportIndex.search(query, 10);

    // Group by city name (no duplicates)
    const seenCities = new Set();
    originSuggestions = results.filter(r => {
        const cityKey = r.city.toLowerCase();
        if (seenCities.has(cityKey)) {
            return false;
        }
        seenCities.add(cityKey);
        return true;
    }).slice(0, 6);

    if (originSuggestions.length === 0) {
        hideOriginAutocomplete();
        return;
    }

    showOriginSuggestions(originSuggestions);
}

/**
 * Show origin autocomplete suggestions
 */
function showOriginSuggestions(suggestions) {
    originSelectedIndex = -1;

    originAutocompleteDropdown.innerHTML = suggestions.map((airport, index) => `
        <div class="origin-autocomplete-item px-4 py-3 cursor-pointer hover:bg-cream-100 flex items-center gap-3 border-b border-cream-100 last:border-0 transition-colors"
             data-index="${index}" data-iata="${airport.iata}">
            <div class="w-8 h-8 bg-terracotta-100 rounded-full flex items-center justify-center flex-shrink-0">
                <i class="fa-solid fa-plane-departure text-terracotta-500 text-sm"></i>
            </div>
            <div class="flex-1 min-w-0">
                <div class="font-semibold text-navy-800">${airport.city}</div>
                <div class="text-sm text-navy-500">${airport.country}</div>
            </div>
        </div>
    `).join('');

    // Add click handlers
    originAutocompleteDropdown.querySelectorAll('.origin-autocomplete-item').forEach(item => {
        item.addEventListener('mousedown', (e) => {
            e.preventDefault();
            const index = parseInt(item.dataset.index);
            selectOriginSuggestion(index);
        });
    });

    originAutocompleteDropdown.classList.remove('hidden');
}

/**
 * Hide origin autocomplete
 */
function hideOriginAutocomplete() {
    originAutocompleteDropdown.classList.add('hidden');
    originSuggestions = [];
    originSelectedIndex = -1;
}

/**
 * Handle origin keyboard navigation
 */
function handleOriginKeyDown(e) {
    if (originAutocompleteDropdown.classList.contains('hidden')) {
        return;
    }

    switch (e.key) {
        case 'ArrowDown':
            e.preventDefault();
            originSelectedIndex = Math.min(originSelectedIndex + 1, originSuggestions.length - 1);
            updateOriginSelectedSuggestion();
            break;
        case 'ArrowUp':
            e.preventDefault();
            originSelectedIndex = Math.max(originSelectedIndex - 1, 0);
            updateOriginSelectedSuggestion();
            break;
        case 'Enter':
            e.preventDefault();
            if (originSelectedIndex >= 0) {
                selectOriginSuggestion(originSelectedIndex);
            } else if (originSuggestions.length > 0) {
                selectOriginSuggestion(0);
            }
            break;
        case 'Escape':
            hideOriginAutocomplete();
            originInput.blur();
            break;
    }
}

/**
 * Update visual selection in origin dropdown
 */
function updateOriginSelectedSuggestion() {
    const items = originAutocompleteDropdown.querySelectorAll('.origin-autocomplete-item');
    items.forEach((item, index) => {
        if (index === originSelectedIndex) {
            item.classList.add('bg-cream-100');
            item.scrollIntoView({ block: 'nearest' });
        } else {
            item.classList.remove('bg-cream-100');
        }
    });
}

/**
 * Select origin suggestion
 */
function selectOriginSuggestion(index) {
    const airport = originSuggestions[index];
    if (airport) {
        originInput.value = airport.city;
        originIataInput.value = airport.iata;
        hideOriginAutocomplete();

        // Show check icon instead of search
        originSearchIcon.classList.add('hidden');
        originCheckIcon.classList.remove('hidden');
    }
}

/**
 * Handle origin input focus
 */
function handleOriginFocus(e) {
    if (e.target.value.trim().length >= 1 && !originIataInput.value) {
        handleOriginInputChange(e);
    }
}

/**
 * Handle origin input blur
 */
function handleOriginBlur() {
    setTimeout(() => hideOriginAutocomplete(), 150);
}

/**
 * Get selected origin IATA code
 * @returns {string} Origin IATA code or empty string
 */
function getOriginIata() {
    return originIataInput.value || '';
}

/**
 * Handle input changes - search and show suggestions
 */
function handleInputChange(e) {
    const query = e.target.value.trim();

    if (query.length < 1) {
        hideAutocomplete();
        return;
    }

    // Search airports
    const results = window.AirportIndex.search(query, 10);

    // Filter out already selected cities and group by city name
    const selectedCityNames = getSelectedCities().map(c => c.toLowerCase());
    const seenCities = new Set();

    currentSuggestions = results.filter(r => {
        const cityKey = r.city.toLowerCase();
        // Skip if city already selected or already in suggestions
        if (selectedCityNames.includes(cityKey) || seenCities.has(cityKey)) {
            return false;
        }
        seenCities.add(cityKey);
        return true;
    }).slice(0, 6);

    if (currentSuggestions.length === 0) {
        hideAutocomplete();
        return;
    }

    showSuggestions(currentSuggestions);
}

/**
 * Show autocomplete suggestions
 */
function showSuggestions(suggestions) {
    selectedSuggestionIndex = -1;

    autocompleteDropdown.innerHTML = suggestions.map((airport, index) => `
        <div class="autocomplete-item px-4 py-3 cursor-pointer hover:bg-cream-100 flex items-center gap-3 border-b border-cream-100 last:border-0 transition-colors"
             data-index="${index}" data-iata="${airport.iata}">
            <div class="w-8 h-8 bg-terracotta-100 rounded-full flex items-center justify-center flex-shrink-0">
                <i class="fa-solid fa-location-dot text-terracotta-500 text-sm"></i>
            </div>
            <div class="flex-1 min-w-0">
                <div class="font-semibold text-navy-800">${airport.city}</div>
                <div class="text-sm text-navy-500">${airport.country}</div>
            </div>
        </div>
    `).join('');

    // Add click handlers
    autocompleteDropdown.querySelectorAll('.autocomplete-item').forEach(item => {
        item.addEventListener('mousedown', (e) => {
            e.preventDefault(); // Prevent blur
            const index = parseInt(item.dataset.index);
            selectSuggestion(index);
        });
    });

    autocompleteDropdown.classList.remove('hidden');
}

/**
 * Hide autocomplete dropdown
 */
function hideAutocomplete() {
    autocompleteDropdown.classList.add('hidden');
    currentSuggestions = [];
    selectedSuggestionIndex = -1;
}

/**
 * Handle keyboard navigation
 */
function handleKeyDown(e) {
    if (autocompleteDropdown.classList.contains('hidden')) {
        return;
    }

    switch (e.key) {
        case 'ArrowDown':
            e.preventDefault();
            selectedSuggestionIndex = Math.min(selectedSuggestionIndex + 1, currentSuggestions.length - 1);
            updateSelectedSuggestion();
            break;
        case 'ArrowUp':
            e.preventDefault();
            selectedSuggestionIndex = Math.max(selectedSuggestionIndex - 1, 0);
            updateSelectedSuggestion();
            break;
        case 'Enter':
            e.preventDefault();
            if (selectedSuggestionIndex >= 0) {
                selectSuggestion(selectedSuggestionIndex);
            } else if (currentSuggestions.length > 0) {
                selectSuggestion(0);
            }
            break;
        case 'Escape':
            hideAutocomplete();
            cityInput.blur();
            break;
    }
}

/**
 * Update visual selection in dropdown
 */
function updateSelectedSuggestion() {
    const items = autocompleteDropdown.querySelectorAll('.autocomplete-item');
    items.forEach((item, index) => {
        if (index === selectedSuggestionIndex) {
            item.classList.add('bg-cream-100');
            item.scrollIntoView({ block: 'nearest' });
        } else {
            item.classList.remove('bg-cream-100');
        }
    });
}

/**
 * Select a suggestion and add it
 */
function selectSuggestion(index) {
    const airport = currentSuggestions[index];
    if (airport) {
        addCity(airport.city, airport.iata);
        cityInput.value = '';
        hideAutocomplete();
    }
}

/**
 * Handle input focus
 */
function handleInputFocus(e) {
    if (e.target.value.trim().length >= 1) {
        handleInputChange(e);
    }
}

/**
 * Handle input blur
 */
function handleInputBlur(e) {
    // Delay to allow click on suggestion
    setTimeout(() => hideAutocomplete(), 150);
}

/**
 * Update the city count display and summary
 */
function updateCityCount() {
    const cities = selectedCities.querySelectorAll('.city-tag');
    const count = cities.length;
    cityCount.textContent = count;
    summaryCities.textContent = count + (count === 1 ? ' city' : ' cities');

    if (count === 0) {
        emptyCitiesHint.classList.remove('hidden');
    } else {
        emptyCitiesHint.classList.add('hidden');
    }
}

/**
 * Get list of currently selected city names
 * @returns {string[]} Array of city names
 */
function getSelectedCities() {
    const cityTags = selectedCities.querySelectorAll('.city-tag');
    return Array.from(cityTags).map(tag => {
        const cityName = tag.querySelector('.city-name');
        return cityName ? cityName.textContent.trim() : '';
    }).filter(name => name.length > 0);
}

/**
 * Get list of currently selected IATA codes
 * @returns {string[]} Array of IATA codes
 */
function getSelectedIatas() {
    const cityTags = selectedCities.querySelectorAll('.city-tag');
    return Array.from(cityTags).map(tag => tag.dataset.iata || '').filter(iata => iata.length > 0);
}

/**
 * Add a city to the selection
 * @param {string} cityName - Name of the city to add
 * @param {string} iata - IATA code of the airport
 */
function addCity(cityName, iata) {
    const trimmedName = cityName.trim();
    if (!trimmedName || !iata) return;

    // Check for duplicates
    const existingIatas = getSelectedIatas();
    if (existingIatas.includes(iata)) {
        cityInput.classList.add('ring-2', 'ring-terracotta-400');
        setTimeout(() => cityInput.classList.remove('ring-2', 'ring-terracotta-400'), 1000);
        return;
    }

    // Create city tag
    const colorIndex = selectedCities.querySelectorAll('.city-tag').length % cityColors.length;
    const cityTag = document.createElement('div');
    cityTag.className = `city-tag flex items-center gap-2 px-4 py-2 ${cityColors[colorIndex]} rounded-full text-sm font-medium transition-transform hover:scale-105`;
    cityTag.dataset.iata = iata;
    cityTag.innerHTML = `
        <span class="w-2 h-2 bg-current opacity-50 rounded-full"></span>
        <span class="city-name">${trimmedName}</span>
        <button type="button" class="remove-city hover:opacity-70 transition-opacity ml-1" aria-label="Remove ${trimmedName}">
            <i class="fa-solid fa-xmark text-xs"></i>
        </button>
    `;

    // Add remove button listener
    cityTag.querySelector('.remove-city').addEventListener('click', function() {
        cityTag.classList.add('scale-0');
        setTimeout(() => {
            cityTag.remove();
            updateCityCount();
        }, 150);
    });

    // Add to container with animation
    selectedCities.appendChild(cityTag);
    updateCityCount();

    // Animate entrance
    cityTag.classList.add('scale-0');
    setTimeout(() => cityTag.classList.remove('scale-0'), 10);
}

/**
 * Update the trip duration display
 */
function updateDuration() {
    const start = new Date(startDateInput.value);
    const end = new Date(endDateInput.value);

    if (startDateInput.value && endDateInput.value && end >= start) {
        const diffTime = Math.abs(end - start);
        const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24)) + 1;
        summaryDuration.textContent = diffDays + (diffDays === 1 ? ' day' : ' days');
    } else {
        summaryDuration.textContent = '-- days';
    }
}

// Date validation
startDateInput.addEventListener('change', () => {
    if (endDateInput.value && new Date(endDateInput.value) < new Date(startDateInput.value)) {
        endDateInput.value = startDateInput.value;
    }
    endDateInput.min = startDateInput.value;
    updateDuration();
});

endDateInput.addEventListener('change', updateDuration);

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    // Wait for AirportIndex to be available
    const checkAirportIndex = setInterval(() => {
        if (window.AirportIndex && window.AirportIndex.isLoaded()) {
            clearInterval(checkAirportIndex);
            initAutocomplete();
            initOriginAutocomplete();
        }
    }, 50);

    // Initialize dates (default: 30 days from now, 5 day trip)
    const today = new Date();
    const startDate = new Date(today.getTime() + 30 * 24 * 60 * 60 * 1000);
    const endDate = new Date(startDate.getTime() + 4 * 24 * 60 * 60 * 1000);

    startDateInput.value = startDate.toISOString().split('T')[0];
    endDateInput.value = endDate.toISOString().split('T')[0];
    startDateInput.min = today.toISOString().split('T')[0];
    endDateInput.min = startDateInput.value;

    updateDuration();
    updateCityCount();
});

// Export for use in other modules
window.CitySelector = {
    getSelectedCities,
    getSelectedIatas,
    getOriginIata,
    addCity,
    updateCityCount
};
