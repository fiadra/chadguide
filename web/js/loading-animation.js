/**
 * Generative Console Loading Animation
 *
 * A sophisticated loading visualization that transforms wait time into value demonstration.
 * Uses Leaflet.js for map visualization with 4-stage progressive animation.
 *
 * KEY PRINCIPLE: Shows the user's ACTUAL selected airports and simulates the optimization
 * process - but does NOT show a final route (that comes from the real calculation).
 *
 * Stage 1 (0-2s):    "The Scatter" - All database airports appear, user selections highlighted
 * Stage 2 (2-4s):    "The Filter" - Non-selected airports fade, destinations pulse
 * Stage 3 (4-6s):    "The Thread" - Possible route orderings are "tested" (dashed lines)
 * Stage 4 (6-7.5s):  "The Lock" - Destinations lock in, ready for results
 */

class GenerativeConsole {
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.container = null; // Will be resolved in init()
        this.options = {
            noisePointCount: 60, // Additional airports to show as "search space"
            ...options
        };

        this.map = null;
        this.markers = [];
        this.userMarkers = [];  // Origin + destinations
        this.noiseMarkers = []; // Other airports
        this.routeLines = [];
        this.currentStage = 0;
        this.animationFrame = null;
        this.stageTimers = [];

        // Stage definitions
        this.stages = [
            {
                duration: 4000,
                title: "Mapping Flight Network",
                getMessage: (ctx) => `Analyzing ${ctx.pointCount}+ routes across European carriers...`,
                icon: "fa-globe-europe"
            },
            {
                duration: 2000,
                title: "Filtering Connections",
                getMessage: (ctx) => `Eliminating ${Math.floor(ctx.pointCount * 0.7)}+ suboptimal routes...`,
                icon: "fa-filter"
            },
            {
                duration: 2000,
                title: "Optimizing Sequence",
                getMessage: (ctx) => `Comparing ${ctx.permutations} possible city orderings...`,
                icon: "fa-diagram-project"
            },
            {
                duration: 1500,
                title: "Verifying Availability",
                getMessage: (ctx) => `Confirming live prices with airlines...`,
                icon: "fa-shield-check"
            }
        ];
    }

    /**
     * Initialize the loading animation with user's actual selections
     * @param {Object} context - Contains origin, destinations, budget, etc.
     */
    init(context = {}) {
        // Resolve container now (DOM should be ready)
        this.container = document.getElementById(this.containerId);

        // Calculate dynamic route count based on search complexity
        const destCount = (context.destinations || []).length;
        const airportCount = window.AirportIndex.getAll().length;
        // More destinations = more routes to analyze
        // Formula: airports * (destinations + 1) * multiplier + randomness
        const baseRoutes = airportCount * (destCount + 1) * 8;
        const routeVariation = Math.floor(Math.random() * baseRoutes * 0.3);

        this.context = {
            origin: context.origin || null,           // IATA code
            destinations: context.destinations || [], // Array of IATA codes
            budget: context.budget || 'moderate',
            duration: context.duration || '5 days',
            cityCount: destCount,
            pointCount: baseRoutes + routeVariation,
            permutations: this._factorial(destCount)
        };

        // Get airport data for origin and destinations
        this.originAirport = window.AirportIndex.getByIata(this.context.origin);
        this.destinationAirports = this.context.destinations
            .map(iata => window.AirportIndex.getByIata(iata))
            .filter(a => a && a.lat && a.lng);

        // Debug logging
        console.log('GenerativeConsole init:', {
            containerId: this.containerId,
            container: !!this.container,
            origin: this.context.origin,
            originAirport: this.originAirport,
            destinations: this.context.destinations,
            destinationAirports: this.destinationAirports.length
        });

        // Check if Leaflet is available
        if (typeof L === 'undefined') {
            console.error('Leaflet (L) is not loaded!');
            return this;
        }

        // Create map if container exists and we have valid origin with coordinates
        if (this.container && this.originAirport && this.originAirport.lat) {
            try {
                this._initMap();
                console.log('Map initialized successfully');
            } catch (e) {
                console.error('Failed to initialize map:', e);
            }
        } else {
            console.warn('Cannot initialize map:', {
                hasContainer: !!this.container,
                hasOrigin: !!this.originAirport,
                hasCoords: this.originAirport?.lat
            });
        }

        return this;
    }

    _factorial(n) {
        if (n <= 1) return 1;
        if (n > 10) return '3.6M+'; // Cap display for large numbers
        let result = 1;
        for (let i = 2; i <= n; i++) result *= i;
        return result.toLocaleString();
    }

    _initMap() {
        // Clear existing map
        if (this.map) {
            this.map.remove();
        }

        // Ensure container has dimensions
        if (!this.container.offsetHeight) {
            console.warn('Map container has no height, forcing dimensions');
            this.container.style.height = '200px';
        }

        // Calculate bounds to fit origin + all destinations
        const allAirports = [this.originAirport, ...this.destinationAirports];
        const lats = allAirports.map(a => a.lat);
        const lngs = allAirports.map(a => a.lng);

        const centerLat = (Math.min(...lats) + Math.max(...lats)) / 2;
        const centerLng = (Math.min(...lngs) + Math.max(...lngs)) / 2;

        console.log('Creating map at center:', centerLat, centerLng);

        // Create map with dark theme
        this.map = L.map(this.container, {
            center: [centerLat, centerLng],
            zoom: 4,
            zoomControl: false,
            attributionControl: false,
            dragging: false,
            scrollWheelZoom: false,
            doubleClickZoom: false,
            touchZoom: false
        });

        // Dark map tiles
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            subdomains: 'abcd',
            maxZoom: 19
        }).addTo(this.map);

        // Fit bounds with padding
        const bounds = L.latLngBounds(allAirports.map(a => [a.lat, a.lng]));
        this.map.fitBounds(bounds, { padding: [40, 40] });

        // Force Leaflet to recalculate size (fixes rendering issues)
        setTimeout(() => {
            if (this.map) {
                this.map.invalidateSize();
                console.log('Map size invalidated');
            }
        }, 100);
    }

    /**
     * Start the animation sequence
     */
    start(onComplete, onStageChange) {
        this.onComplete = onComplete;
        this.onStageChange = onStageChange;
        this.currentStage = 0;

        // Clear any existing animations
        this._cleanup();

        // Start stage 1
        this._runStage(0);
    }

    _runStage(stageIndex) {
        if (stageIndex >= this.stages.length) {
            // Animation complete
            setTimeout(() => {
                if (this.onComplete) this.onComplete();
            }, 500);
            return;
        }

        const stage = this.stages[stageIndex];
        this.currentStage = stageIndex;

        // Notify stage change
        if (this.onStageChange) {
            this.onStageChange({
                stage: stageIndex,
                title: stage.title,
                message: stage.getMessage(this.context),
                icon: stage.icon,
                progress: Math.round(((stageIndex + 1) / this.stages.length) * 100)
            });
        }

        // Run stage animation
        switch (stageIndex) {
            case 0:
                this._animateScatter();
                break;
            case 1:
                this._animateFilter();
                break;
            case 2:
                this._animateThread();
                break;
            case 3:
                this._animateLock();
                break;
        }

        // Schedule next stage
        const timer = setTimeout(() => {
            this._runStage(stageIndex + 1);
        }, stage.duration);

        this.stageTimers.push(timer);
    }

    /**
     * Stage 1: The Scatter
     * Shows all airports in database, with user's selections already visible
     */
    _animateScatter() {
        if (!this.map) return;

        // Get all airports for "noise" effect
        const allAirports = window.AirportIndex.getAll();
        const userIatas = [this.context.origin, ...this.context.destinations];

        // Filter out user selections for noise
        const noiseAirports = allAirports
            .filter(a => !userIatas.includes(a.iata) && a.lat && a.lng)
            .slice(0, this.options.noisePointCount);

        // Add noise points with staggered animation
        noiseAirports.forEach((airport, i) => {
            const delay = (i / noiseAirports.length) * 1200;

            setTimeout(() => {
                if (!this.map) return;

                const marker = L.circleMarker([airport.lat, airport.lng], {
                    radius: 3,
                    fillColor: '#f2cc8f',
                    fillOpacity: 0.5,
                    color: 'transparent',
                    weight: 0,
                    className: 'scatter-point noise-point'
                }).addTo(this.map);

                this.noiseMarkers.push(marker);
            }, delay);
        });

        // Add user's origin (terracotta - stands out)
        setTimeout(() => {
            if (!this.map || !this.originAirport) return;

            const originMarker = L.circleMarker([this.originAirport.lat, this.originAirport.lng], {
                radius: 8,
                fillColor: '#e07a5f',
                fillOpacity: 1,
                color: '#fff',
                weight: 2,
                className: 'scatter-point origin-point'
            }).addTo(this.map);

            // Add label
            originMarker.bindTooltip(this.originAirport.city, {
                permanent: true,
                direction: 'top',
                className: 'airport-label origin-label',
                offset: [0, -10]
            });

            this.userMarkers.push({ marker: originMarker, type: 'origin', airport: this.originAirport });
        }, 800);

        // Add user's destinations (sage - will highlight later)
        this.destinationAirports.forEach((airport, i) => {
            const delay = 1000 + (i * 150);

            setTimeout(() => {
                if (!this.map) return;

                const marker = L.circleMarker([airport.lat, airport.lng], {
                    radius: 6,
                    fillColor: '#81b29a',
                    fillOpacity: 0.7,
                    color: '#81b29a',
                    weight: 1,
                    className: 'scatter-point dest-point'
                }).addTo(this.map);

                this.userMarkers.push({ marker, type: 'destination', airport });
            }, delay);
        });
    }

    /**
     * Stage 2: The Filter
     * Noise fades out, user selections pulse and get labels
     */
    _animateFilter() {
        if (!this.map) return;

        // Fade out noise markers
        this.noiseMarkers.forEach((marker, i) => {
            setTimeout(() => {
                marker.setStyle({
                    fillOpacity: 0.1,
                    radius: 2
                });
            }, i * 20);
        });

        // Highlight user destinations with pulse and labels
        this.userMarkers.forEach((item, i) => {
            if (item.type === 'destination') {
                setTimeout(() => {
                    item.marker.setStyle({
                        radius: 8,
                        fillColor: '#81b29a',
                        fillOpacity: 1,
                        color: '#fff',
                        weight: 2
                    });

                    // Add pulse effect
                    const el = item.marker.getElement();
                    if (el) el.classList.add('pulse-marker');

                    // Add label
                    item.marker.bindTooltip(item.airport.city, {
                        permanent: true,
                        direction: 'top',
                        className: 'airport-label dest-label',
                        offset: [0, -10]
                    });
                }, 200 + i * 200);
            }
        });
    }

    /**
     * Stage 3: The Thread
     * Shows different route orderings being "tested" as dashed lines
     */
    _animateThread() {
        if (!this.map) return;

        const destinations = this.userMarkers.filter(m => m.type === 'destination');
        if (destinations.length < 2) return;

        // Generate a few different orderings to "test"
        const orderings = this._generateOrderings(destinations, 3);

        let orderingIndex = 0;

        const testNextOrdering = () => {
            if (orderingIndex >= orderings.length || this.currentStage !== 2) return;

            // Clear previous test lines
            this.routeLines.forEach(line => line.remove());
            this.routeLines = [];

            const ordering = orderings[orderingIndex];

            // Draw lines from origin through this ordering
            let prevPoint = [this.originAirport.lat, this.originAirport.lng];

            ordering.forEach((item, i) => {
                const toPoint = [item.airport.lat, item.airport.lng];

                setTimeout(() => {
                    if (!this.map || this.currentStage !== 2) return;

                    const line = L.polyline([prevPoint, toPoint], {
                        color: '#e07a5f',
                        weight: 2,
                        opacity: 0.7,
                        dashArray: '8, 8',
                        className: 'route-testing'
                    }).addTo(this.map);

                    this.routeLines.push(line);
                }, i * 120);

                prevPoint = toPoint;
            });

            orderingIndex++;

            // Test next ordering after a delay
            if (orderingIndex < orderings.length) {
                setTimeout(testNextOrdering, 600);
            }
        };

        testNextOrdering();
    }

    _generateOrderings(items, count) {
        const orderings = [];

        for (let i = 0; i < count; i++) {
            // Create a shuffled copy
            const shuffled = [...items].sort(() => Math.random() - 0.5);
            orderings.push(shuffled);
        }

        return orderings;
    }

    /**
     * Stage 4: The Lock
     * Destinations "lock in" - ready state. NO final route shown.
     */
    _animateLock() {
        if (!this.map) return;

        // Fade out all test route lines
        this.routeLines.forEach(line => {
            line.setStyle({ opacity: 0 });
        });

        // Lock in each destination with a "confirmation" effect
        this.userMarkers.forEach((item, i) => {
            setTimeout(() => {
                if (item.type === 'destination') {
                    // Solid locked-in style
                    item.marker.setStyle({
                        radius: 10,
                        fillColor: '#81b29a',
                        fillOpacity: 1,
                        color: '#fff',
                        weight: 3
                    });

                    // Remove pulse, add locked class
                    const el = item.marker.getElement();
                    if (el) {
                        el.classList.remove('pulse-marker');
                        el.classList.add('locked-marker');
                    }
                } else if (item.type === 'origin') {
                    // Origin gets final styling
                    item.marker.setStyle({
                        radius: 10,
                        fillColor: '#e07a5f',
                        fillOpacity: 1,
                        color: '#fff',
                        weight: 3
                    });
                }
            }, i * 150);
        });

        // Fit bounds to just user selections
        setTimeout(() => {
            if (!this.map) return;

            const userBounds = L.latLngBounds(
                this.userMarkers.map(m => m.marker.getLatLng())
            );

            this.map.fitBounds(userBounds, {
                padding: [50, 50],
                animate: true,
                duration: 0.5
            });
        }, 500);
    }

    /**
     * Get current progress percentage
     */
    getProgress() {
        const totalDuration = this.stages.reduce((sum, s) => sum + s.duration, 0);
        const completedDuration = this.stages
            .slice(0, this.currentStage)
            .reduce((sum, s) => sum + s.duration, 0);

        return Math.round((completedDuration / totalDuration) * 100);
    }

    /**
     * Extend current stage duration (called when backend needs more time)
     * Doubles the remaining duration of the current stage
     */
    extendCurrentStage() {
        if (this.currentStage >= this.stages.length) return;

        // Clear the last scheduled timer (which advances to next stage)
        const lastTimer = this.stageTimers.pop();
        if (lastTimer) {
            clearTimeout(lastTimer);
        }

        // Get extra time (same as current stage duration)
        const extraTime = this.stages[this.currentStage].duration;

        console.log(`Extending stage ${this.currentStage} by ${extraTime}ms`);

        // Schedule new timer to advance to next stage
        const newTimer = setTimeout(() => {
            this._runStage(this.currentStage + 1);
        }, extraTime);

        this.stageTimers.push(newTimer);
    }

    /**
     * EVENT-DRIVEN MODE
     * Animation runs stages 0-1 with random timing, then WAITS for backend signal
     * to advance to stages 2-3, then WAITS for complete signal
     */
    startEventDriven(onStageChange) {
        this.onStageChange = onStageChange;
        this.currentStage = 0;
        this.isEventDriven = true;
        this.waitingAtBoundary = false;
        this.pendingCompletion = null;

        // Clear any existing animations
        this._cleanup();

        // Run stages 0-1 with randomized longer timing
        this._runEventDrivenStage(0);
    }

    /**
     * Run a stage in event-driven mode with random timing
     * Stages 0-1: routing phase, will WAIT after stage 1
     * Stages 2-3: validation phase, will WAIT after stage 3
     */
    _runEventDrivenStage(stageIndex) {
        if (stageIndex >= this.stages.length) {
            // Animation complete - wait for completeAndFinish() call
            console.log('Animation reached final state, waiting for backend complete signal');
            this.waitingAtBoundary = true;
            return;
        }

        const stage = this.stages[stageIndex];
        this.currentStage = stageIndex;

        // Notify stage change
        if (this.onStageChange) {
            this.onStageChange({
                stage: stageIndex,
                title: stage.title,
                message: stage.getMessage(this.context),
                icon: stage.icon,
                progress: Math.round(((stageIndex + 1) / this.stages.length) * 100)
            });
        }

        // Run stage animation
        switch (stageIndex) {
            case 0: this._animateScatter(); break;
            case 1: this._animateFilter(); break;
            case 2: this._animateThread(); break;
            case 3: this._animateLock(); break;
        }

        // Randomized duration: 2-5 seconds per stage for more natural feel
        const baseDuration = stage.duration;
        const randomDuration = baseDuration + Math.random() * 3000; // +0-3 seconds

        console.log(`Stage ${stageIndex}: "${stage.title}" - running for ${Math.round(randomDuration)}ms`);

        // Check if we should WAIT at a boundary
        if (stageIndex === 1) {
            // After stage 1 (Filter), WAIT for backend "validating" signal
            const timer = setTimeout(() => {
                console.log('Stage 1 complete - WAITING for backend validating signal');
                this.waitingAtBoundary = true;
                // Don't advance - wait for advanceToStage(2) call
            }, randomDuration);
            this.stageTimers.push(timer);
        } else {
            // Normal progression for other stages
            const timer = setTimeout(() => {
                this._runEventDrivenStage(stageIndex + 1);
            }, randomDuration);
            this.stageTimers.push(timer);
        }
    }

    /**
     * Force animation to advance to a specific stage (called by backend signal)
     * Used when backend signals "validating" to advance to stage 2
     */
    advanceToStage(targetStage) {
        console.log(`advanceToStage(${targetStage}) called, current stage: ${this.currentStage}`);

        // Clear waiting state
        this.waitingAtBoundary = false;

        // If we're already past this stage, ignore
        if (this.currentStage >= targetStage) {
            console.log(`Already at or past stage ${targetStage}, ignoring`);
            return;
        }

        // Clear any pending timers
        this.stageTimers.forEach(timer => clearTimeout(timer));
        this.stageTimers = [];

        // Jump to target stage
        this._runEventDrivenStage(targetStage);
    }

    /**
     * Complete the animation gracefully and call the callback
     * Called when backend signals "complete"
     */
    completeAndFinish(callback) {
        console.log('completeAndFinish called, current stage:', this.currentStage);

        // If animation hasn't finished yet, let it complete naturally then redirect
        if (this.currentStage < this.stages.length - 1) {
            // Animation is still running - mark pending completion
            console.log('Animation still running, will complete after current stages');
            this.pendingCompletion = callback;

            // Advance to final stage if stuck at boundary
            if (this.waitingAtBoundary) {
                this.advanceToStage(this.currentStage + 1);
            }

            // Set a timer to force completion after remaining stages
            const remainingStages = this.stages.length - this.currentStage;
            const maxWait = remainingStages * 3000; // 3 seconds max per remaining stage

            setTimeout(() => {
                if (this.pendingCompletion) {
                    console.log('Forcing completion after timeout');
                    this.pendingCompletion();
                    this.pendingCompletion = null;
                }
            }, maxWait);
            return;
        }

        // Animation is at final stage (or past it)
        if (this.waitingAtBoundary) {
            // Was waiting for us - give a brief moment for final visuals
            setTimeout(() => {
                callback();
            }, 800);
        } else {
            // Animation might still be in final stage - wait a bit
            setTimeout(() => {
                callback();
            }, 1500);
        }
    }

    /**
     * Stop and cleanup
     */
    stop() {
        this._cleanup();
    }

    _cleanup() {
        // Clear timers
        this.stageTimers.forEach(timer => clearTimeout(timer));
        this.stageTimers = [];

        // Cancel animation frame
        if (this.animationFrame) {
            cancelAnimationFrame(this.animationFrame);
            this.animationFrame = null;
        }

        // Clear map layers
        if (this.map) {
            this.userMarkers.forEach(m => m.marker.remove());
            this.noiseMarkers.forEach(m => m.remove());
            this.routeLines.forEach(l => l.remove());
        }

        this.userMarkers = [];
        this.noiseMarkers = [];
        this.routeLines = [];
    }

    /**
     * Destroy the console
     */
    destroy() {
        this._cleanup();
        if (this.map) {
            this.map.remove();
            this.map = null;
        }
    }
}

// Export for use
window.GenerativeConsole = GenerativeConsole;
