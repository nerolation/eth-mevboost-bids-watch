// MEV Builder Bids Dashboard - Frontend Logic

class MEVDashboard {
    constructor() {
        // State
        this.currentSlot = null;
        this.latestSlot = null;
        this.headOffset = 100;  // Buffer from chain head
        this.maxAvailableSlot = null;  // Latest slot we can safely display
        this.isPlaying = true;
        this.isTransitioning = false;  // Prevent multiple slot transitions
        this.timerValue = 12;
        this.elapsedTime = 0;  // Time elapsed in current slot display
        this.timerInterval = null;
        this.allBidsData = [];  // All bids for current slot
        this.bidsData = [];     // Currently visible bids (filtered by time)
        this.relaysData = [];   // Relays that reported bids for current slot
        this.builderColors = new Map();
        this.MAX_BUILDER_COLORS = 200;  // Limit to prevent memory growth
        this.chartInitialized = false;

        // Cache for slot data
        this.slotCache = new Map();
        this.pendingFetches = new Set();
        this.PREFETCH_COUNT = 5;  // Number of slots to prefetch ahead

        // DOM Elements
        this.elements = {
            slotNumber: document.getElementById('slotNumber'),
            timerText: document.getElementById('timerText'),
            timerProgress: document.getElementById('timerProgress'),
            playPauseBtn: document.getElementById('playPauseBtn'),
            prevBtn: document.getElementById('prevBtn'),
            nextBtn: document.getElementById('nextBtn'),
            chart: document.getElementById('chart'),
            chartWrapper: document.querySelector('.chart-wrapper'),
            totalBids: document.getElementById('totalBids'),
            maxBid: document.getElementById('maxBid'),
            builderCount: document.getElementById('builderCount'),
            legendList: document.getElementById('legendList'),
            relaysList: document.getElementById('relaysList'),
            winningBuilder: document.getElementById('winningBuilder'),
            winningLabel: document.querySelector('.winning-label'),
            winningColor: document.getElementById('winningColor'),
            winningName: document.getElementById('winningName'),
            winningValue: document.getElementById('winningValue'),
            statusIndicator: document.getElementById('statusIndicator'),
            statusText: document.querySelector('.status-text'),
            // Loading overlay elements
            loadingOverlay: document.getElementById('loadingOverlay'),
            loadingStatus: document.getElementById('loadingStatus'),
            loadingProgressBar: document.getElementById('loadingProgressBar')
        };

        // Track initial load state
        this.initialLoadComplete = false;

        // Plotly chart configuration
        this.chartLayout = {
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            font: {
                family: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
                color: '#94a3b8'
            },
            xaxis: {
                title: {
                    text: 'Time in Slot (seconds)',
                    font: { size: 12, color: '#94a3b8' }
                },
                range: [0, 4],  // Will be updated dynamically
                gridcolor: 'rgba(148,163,184,0.08)',
                zerolinecolor: 'rgba(148,163,184,0.15)',
                tickfont: { size: 11 }
            },
            yaxis: {
                title: {
                    text: 'Bid Value (ETH)',
                    font: { size: 12, color: '#94a3b8' }
                },
                gridcolor: 'rgba(148,163,184,0.08)',
                zerolinecolor: 'rgba(148,163,184,0.15)',
                tickfont: { size: 11 },
                tickformat: '.4f'
            },
            margin: { l: 70, r: 30, t: 30, b: 60 },
            showlegend: false,
            hovermode: 'closest'
        };

        this.chartConfig = {
            responsive: true,
            displayModeBar: false
        };

        // Initialize
        this.init();
    }

    async init() {
        this.setupEventListeners();
        this.setupTimerSVG();
        await this.fetchLatestSlot();
        this.startTimer();
    }

    // --- Loading Overlay Methods ---

    updateLoadingStatus(message, progress = null) {
        if (this.elements.loadingStatus) {
            this.elements.loadingStatus.textContent = message;
        }
        if (progress !== null && this.elements.loadingProgressBar) {
            this.elements.loadingProgressBar.style.width = `${progress}%`;
        }
    }

    hideLoadingOverlay() {
        if (this.elements.loadingOverlay && !this.initialLoadComplete) {
            this.initialLoadComplete = true;
            this.elements.loadingOverlay.classList.add('hidden');
        }
    }

    setupEventListeners() {
        this.elements.playPauseBtn.addEventListener('click', () => this.togglePlayPause());
        this.elements.prevBtn.addEventListener('click', () => this.goToPrevSlot());
        this.elements.nextBtn.addEventListener('click', () => this.goToNextSlot());

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.code === 'Space') {
                e.preventDefault();
                this.togglePlayPause();
            } else if (e.code === 'ArrowLeft') {
                this.goToPrevSlot();
            } else if (e.code === 'ArrowRight') {
                this.goToNextSlot();
            }
        });
    }

    setupTimerSVG() {
        // Add gradient definition to the SVG
        const svg = this.elements.timerProgress.closest('svg');
        const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
        defs.innerHTML = `
            <linearGradient id="timerGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                <stop offset="0%" style="stop-color:#38bdf8"/>
                <stop offset="100%" style="stop-color:#818cf8"/>
            </linearGradient>
        `;
        svg.insertBefore(defs, svg.firstChild);
        this.elements.timerProgress.style.stroke = 'url(#timerGradient)';
    }

    // --- Caching and Prefetch Logic ---

    getCachedSlot(slot) {
        return this.slotCache.get(slot);
    }

    setCachedSlot(slot, data) {
        this.slotCache.set(slot, data);
        // Keep cache size reasonable (last 20 slots)
        if (this.slotCache.size > 20) {
            const oldest = this.slotCache.keys().next().value;
            this.slotCache.delete(oldest);
        }
    }

    async fetchSlotData(slot) {
        // Check cache first
        const cached = this.getCachedSlot(slot);
        if (cached) {
            return cached;
        }

        // Don't duplicate fetches
        if (this.pendingFetches.has(slot)) {
            // Wait for existing fetch to complete
            return new Promise((resolve) => {
                const checkCache = setInterval(() => {
                    const data = this.getCachedSlot(slot);
                    if (data) {
                        clearInterval(checkCache);
                        resolve(data);
                    }
                }, 100);
                // Timeout after 10 seconds
                setTimeout(() => {
                    clearInterval(checkCache);
                    resolve(null);
                }, 10000);
            });
        }

        this.pendingFetches.add(slot);

        try {
            const response = await fetch(`/api/slot/${slot}`);
            const data = await response.json();
            this.setCachedSlot(slot, data);
            return data;
        } catch (error) {
            console.error(`Failed to fetch slot ${slot}:`, error);
            return null;
        } finally {
            this.pendingFetches.delete(slot);
        }
    }

    async prefetchSlots(startSlot) {
        // Prefetch next N slots in parallel
        const slotsToFetch = [];
        for (let i = 1; i <= this.PREFETCH_COUNT; i++) {
            const slot = startSlot + i;
            if (!this.getCachedSlot(slot) && !this.pendingFetches.has(slot)) {
                slotsToFetch.push(slot);
            }
        }

        if (slotsToFetch.length > 0) {
            // Fire off prefetch requests without waiting
            slotsToFetch.forEach(slot => {
                this.fetchSlotData(slot).catch(() => {});
            });
        }
    }

    // --- Main Data Loading ---

    async fetchLatestSlot() {
        try {
            this.updateLoadingStatus('Connecting to Xatu...', 10);

            const response = await fetch('/api/latest-slot');
            const data = await response.json();
            this.latestSlot = data.slot;
            this.headOffset = data.head_offset || 100;
            this.maxAvailableSlot = data.slot;  // This is already offset from head
            this.currentSlot = this.latestSlot;

            this.updateLoadingStatus(`Loading slot ${this.currentSlot}...`, 40);
            this.updateStatus('connected', `Connected - Slot ${this.currentSlot}`);

            await this.loadSlotData();

            this.updateLoadingStatus('Ready!', 100);

            // Hide overlay after a brief moment to show "Ready!"
            setTimeout(() => this.hideLoadingOverlay(), 300);

            // Start prefetching immediately
            this.prefetchSlots(this.currentSlot);
        } catch (error) {
            console.error('Failed to fetch latest slot:', error);
            this.updateLoadingStatus('Connection failed. Retrying...', 0);
            this.updateStatus('error', 'Connection failed');

            // Retry after 3 seconds
            setTimeout(() => this.fetchLatestSlot(), 3000);
        }
    }

    async refreshMaxAvailableSlot() {
        try {
            const response = await fetch('/api/latest-slot');
            const data = await response.json();
            this.maxAvailableSlot = data.slot;
            return this.maxAvailableSlot;
        } catch (error) {
            console.error('Failed to refresh max slot:', error);
            return this.maxAvailableSlot;
        }
    }

    async loadSlotData() {
        if (!this.currentSlot) return;

        // Check if we have cached data
        const cached = this.getCachedSlot(this.currentSlot);

        if (cached) {
            // Use cached data immediately - no loading overlay!
            this.displaySlotData(cached);
            this.updateStatus('connected', `Slot ${this.currentSlot} - ${cached.bids?.length || 0} bids (cached)`);
            // Continue prefetching
            this.prefetchSlots(this.currentSlot);
            return;
        }

        // No cache - fetch data
        try {
            this.updateStatus('connected', `Loading slot ${this.currentSlot}...`);

            // Update loading overlay progress during initial load
            if (!this.initialLoadComplete) {
                this.updateLoadingStatus(`Fetching bids for slot ${this.currentSlot}...`, 60);
            }

            const data = await this.fetchSlotData(this.currentSlot);

            if (!this.initialLoadComplete) {
                this.updateLoadingStatus('Processing data...', 85);
            }

            if (data) {
                this.displaySlotData(data);
                this.updateStatus('connected', `Slot ${this.currentSlot} - ${data.bids?.length || 0} bids`);
            } else {
                this.updateStatus('error', `Failed to load slot ${this.currentSlot}`);
            }

            // Start prefetching next slots
            this.prefetchSlots(this.currentSlot);
        } catch (error) {
            console.error('Failed to load slot data:', error);
            this.updateStatus('error', `Failed to load slot ${this.currentSlot}`);
        }
    }

    displaySlotData(data) {
        // Store all bids and reset elapsed time
        this.allBidsData = data.bids || [];
        this.relaysData = data.relays || [];
        this.winningBlockHash = data.winning_block_hash || null;
        this.elapsedTime = 0;
        this.elements.slotNumber.textContent = this.currentSlot.toLocaleString();

        // Update colors map (with LRU eviction to prevent memory growth)
        this.allBidsData.forEach(bid => {
            if (!this.builderColors.has(bid.builder_pubkey)) {
                // Evict oldest entries if at capacity
                if (this.builderColors.size >= this.MAX_BUILDER_COLORS) {
                    const firstKey = this.builderColors.keys().next().value;
                    this.builderColors.delete(firstKey);
                }
                this.builderColors.set(bid.builder_pubkey, bid.color);
            }
        });

        // Update relays display
        this.updateRelays();

        // Filter bids visible at current elapsed time and update display
        this.updateVisibleBids();
    }

    updateRelays() {
        if (!this.elements.relaysList) return;

        if (this.relaysData.length === 0) {
            this.elements.relaysList.innerHTML = '<span class="relay-empty">No relays</span>';
            return;
        }

        this.elements.relaysList.innerHTML = this.relaysData.map(relay => {
            // Clean up relay name for display (remove common prefixes/suffixes)
            const displayName = relay.replace(/^relay-/, '').replace(/-relay$/, '');
            return `<span class="relay-tile" title="${relay}">${displayName}</span>`;
        }).join('');
    }

    updateVisibleBids() {
        // Show only bids that would have arrived by the current elapsed time
        this.bidsData = this.allBidsData.filter(bid => bid.seconds_in_slot <= this.elapsedTime);

        this.updateChart();
        this.updateStats();
        this.updateLegend();
        this.updateWinningBuilder();
    }

    updateWinningBuilder() {
        // Check if the delivered bid is visible
        const deliveredBid = this.bidsData.find(bid => bid.is_winner);

        if (this.bidsData.length === 0) {
            this.elements.winningBuilder.classList.remove('has-winner', 'is-delivered');
            this.elements.winningLabel.textContent = 'Leading';
            this.elements.winningColor.style.backgroundColor = '';
            this.elements.winningColor.style.color = '';
            this.elements.winningColor.classList.remove('active');
            this.elements.winningName.textContent = '--';
            this.elements.winningValue.textContent = '';
            return;
        }

        // Find the highest bid among visible bids
        const leadingBid = this.bidsData.reduce((max, bid) =>
            bid.value_eth > max.value_eth ? bid : max
        );

        this.elements.winningBuilder.classList.add('has-winner');

        // If the delivered bid is visible, show it with special styling
        if (deliveredBid) {
            this.elements.winningBuilder.classList.add('is-delivered');
            this.elements.winningLabel.textContent = 'Delivered';
            this.elements.winningColor.style.backgroundColor = '#fbbf24';  // Gold color
            this.elements.winningColor.style.color = '#fbbf24';
            this.elements.winningColor.classList.add('active');
            this.elements.winningName.textContent = `★ ${deliveredBid.builder_label}`;
            this.elements.winningValue.textContent = `${deliveredBid.value_eth.toFixed(4)} ETH`;
        } else {
            this.elements.winningBuilder.classList.remove('is-delivered');
            this.elements.winningLabel.textContent = 'Leading';
            this.elements.winningColor.style.backgroundColor = leadingBid.color;
            this.elements.winningColor.style.color = leadingBid.color;
            this.elements.winningColor.classList.add('active');
            this.elements.winningName.textContent = leadingBid.builder_label;
            this.elements.winningValue.textContent = `${leadingBid.value_eth.toFixed(4)} ETH`;
        }
    }

    updateChart() {
        // Calculate dynamic x-axis range based on ALL bids (not just visible)
        // This prevents the axis from jumping as bids appear
        let maxX = 4;  // Default minimum
        if (this.allBidsData.length > 0) {
            const maxDataX = Math.max(...this.allBidsData.map(b => b.seconds_in_slot));
            maxX = Math.max(maxX, maxDataX * 1.1);  // Add 10% padding
        }

        // Update layout with dynamic range
        const layout = {
            ...this.chartLayout,
            xaxis: {
                ...this.chartLayout.xaxis,
                range: [0, maxX]
            }
        };

        if (this.bidsData.length === 0) {
            // Show empty state
            const emptyTrace = {
                x: [],
                y: [],
                mode: 'markers',
                type: 'scatter'
            };

            if (!this.chartInitialized) {
                Plotly.newPlot(this.elements.chart, [emptyTrace], layout, this.chartConfig);
                this.chartInitialized = true;
            } else {
                Plotly.react(this.elements.chart, [emptyTrace], layout);
            }
            return;
        }

        // Separate winning bid from regular bids
        const regularBids = this.bidsData.filter(bid => !bid.is_winner);
        const winningBid = this.bidsData.find(bid => bid.is_winner);

        // Group regular bids by builder
        const builderGroups = new Map();
        regularBids.forEach(bid => {
            if (!builderGroups.has(bid.builder_pubkey)) {
                builderGroups.set(bid.builder_pubkey, {
                    x: [],
                    y: [],
                    label: bid.builder_label,
                    color: bid.color
                });
            }
            const group = builderGroups.get(bid.builder_pubkey);
            group.x.push(bid.seconds_in_slot);
            group.y.push(bid.value_eth);
        });

        // Create traces for each builder (regular bids)
        const traces = Array.from(builderGroups.entries()).map(([pubkey, data]) => ({
            x: data.x,
            y: data.y,
            mode: 'markers',
            type: 'scatter',
            name: data.label,
            marker: {
                color: data.color,
                size: 10,
                opacity: 0.85,
                line: {
                    color: 'rgba(255,255,255,0.3)',
                    width: 1
                }
            },
            hovertemplate: `<b>${data.label}</b><br>` +
                           `Time: %{x:.2f}s<br>` +
                           `Value: %{y:.6f} ETH<br>` +
                           `<extra></extra>`
        }));

        // Add winning bid as a special trace with star marker
        if (winningBid) {
            traces.push({
                x: [winningBid.seconds_in_slot],
                y: [winningBid.value_eth],
                mode: 'markers',
                type: 'scatter',
                name: `${winningBid.builder_label} (Delivered)`,
                marker: {
                    symbol: 'star',
                    color: '#fbbf24',  // Gold/amber color
                    size: 18,
                    opacity: 1,
                    line: {
                        color: '#ffffff',
                        width: 2
                    }
                },
                hovertemplate: `<b>⭐ DELIVERED BID</b><br>` +
                               `<b>${winningBid.builder_label}</b><br>` +
                               `Time: %{x:.2f}s<br>` +
                               `Value: %{y:.6f} ETH<br>` +
                               `<extra></extra>`
            });
        }

        if (!this.chartInitialized) {
            Plotly.newPlot(this.elements.chart, traces, layout, this.chartConfig);
            this.chartInitialized = true;
        } else {
            Plotly.react(this.elements.chart, traces, layout);
        }
    }

    updateStats() {
        const totalBids = this.bidsData.length;
        const uniqueBuilders = new Set(this.bidsData.map(b => b.builder_pubkey)).size;
        const maxBid = this.bidsData.length > 0
            ? Math.max(...this.bidsData.map(b => b.value_eth))
            : 0;

        this.elements.totalBids.textContent = totalBids.toLocaleString();
        this.elements.builderCount.textContent = uniqueBuilders;
        this.elements.maxBid.textContent = maxBid > 0 ? `${maxBid.toFixed(4)} ETH` : '--';
    }

    updateLegend() {
        // Count bids per builder (group by pubkey for color consistency)
        const builderCounts = new Map();
        let deliveredPubkey = null;

        this.bidsData.forEach(bid => {
            const pubkey = bid.builder_pubkey;
            if (!builderCounts.has(pubkey)) {
                builderCounts.set(pubkey, {
                    label: bid.builder_label,
                    color: bid.color,
                    count: 0,
                    isDelivered: false
                });
            }
            builderCounts.get(pubkey).count++;
            if (bid.is_winner) {
                builderCounts.get(pubkey).isDelivered = true;
                deliveredPubkey = pubkey;
            }
        });

        // Sort by count descending, but put delivered builder first
        const sorted = Array.from(builderCounts.entries())
            .sort((a, b) => {
                if (a[1].isDelivered) return -1;
                if (b[1].isDelivered) return 1;
                return b[1].count - a[1].count;
            });

        // Generate legend HTML
        this.elements.legendList.innerHTML = sorted.map(([pubkey, data]) => {
            const deliveredClass = data.isDelivered ? 'is-delivered' : '';
            const starIndicator = data.isDelivered ? '<span class="delivered-star">★</span>' : '';
            return `
                <div class="legend-item ${deliveredClass}">
                    <div class="legend-color" style="background-color: ${data.isDelivered ? '#fbbf24' : data.color}; color: ${data.isDelivered ? '#fbbf24' : data.color}"></div>
                    <span class="legend-name" title="${data.label}">${starIndicator}${data.label}</span>
                    <span class="legend-count">${data.count}</span>
                </div>
            `;
        }).join('');
    }

    updateStatus(status, text) {
        this.elements.statusIndicator.className = `status-indicator ${status}`;
        this.elements.statusText.textContent = text;
    }

    startTimer() {
        this.timerValue = 12;
        this.elapsedTime = 0;
        this.updateTimerDisplay();

        if (this.timerInterval) {
            clearInterval(this.timerInterval);
        }

        this.timerInterval = setInterval(async () => {
            if (!this.isPlaying || this.isTransitioning) return;

            this.timerValue -= 0.1;
            this.elapsedTime += 0.1;
            this.updateTimerDisplay();

            // Update visible bids based on elapsed time
            this.updateVisibleBids();

            if (this.timerValue <= 0) {
                this.isTransitioning = true;
                await this.goToNextSlot();
                this.isTransitioning = false;
            }
        }, 100);
    }

    updateTimerDisplay() {
        const displayValue = Math.max(0, Math.ceil(this.timerValue));
        this.elements.timerText.textContent = displayValue;

        // Update progress ring (283 is the circumference of the circle)
        const progress = (1 - this.timerValue / 12) * 283;
        this.elements.timerProgress.style.strokeDashoffset = progress;
    }

    togglePlayPause() {
        this.isPlaying = !this.isPlaying;
        this.elements.playPauseBtn.classList.toggle('playing', this.isPlaying);

        if (this.isPlaying) {
            this.startTimer();
            // Resume prefetching
            this.prefetchSlots(this.currentSlot);
        }
    }

    async goToPrevSlot() {
        if (this.currentSlot > 0) {
            this.currentSlot--;
            this.timerValue = 12;
            this.elapsedTime = 0;
            this.updateTimerDisplay();
            await this.loadSlotData();
        }
    }

    async goToNextSlot() {
        // Simply advance to next slot
        this.currentSlot++;
        this.timerValue = 12;
        this.elapsedTime = 0;
        this.updateTimerDisplay();

        // Update display immediately to show new slot number
        this.elements.slotNumber.textContent = this.currentSlot.toLocaleString();

        try {
            // Load the slot data - if it fails or is empty, that's okay
            await this.loadSlotData();
        } catch (error) {
            console.error('Error loading slot:', error);
            this.updateStatus('error', `Error loading slot ${this.currentSlot}`);
        }

        // Periodically refresh what the max available slot is
        if (this.currentSlot % 10 === 0) {
            this.refreshMaxAvailableSlot();
        }
    }
}

// Initialize dashboard when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new MEVDashboard();
});
