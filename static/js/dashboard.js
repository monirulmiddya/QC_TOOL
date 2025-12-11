/**
 * QC Tool - Dashboard Module
 * Manages the executive dashboard view with KPIs and visualizations
 */

const Dashboard = {
    // Dashboard state
    state: {
        currentView: 'executive', // 'executive' | 'detailed' | 'trends'
        results: null,
        charts: {}
    },

    /**
     * Initialize dashboard
     */
    init() {
        console.log('Dashboard initialized');
    },

    /**
     * Render Executive Dashboard
     */
    renderExecutiveDashboard(results) {
        if (!results || !results.results) return;

        this.state.results = results;
        const container = document.getElementById('dashboardContainer');
        if (!container) return;

        // Calculate KPIs
        const kpis = this.calculateKPIs(results.results);

        // Build dashboard HTML
        container.innerHTML = `
            <div class="dashboard-executive">
                <!-- KPI Cards -->
                <div class="kpi-grid">
                    ${this.createKPICard('Total Checks', kpis.totalChecks, 'check-circle', kpis.totalChange)}
                    ${this.createKPICard('Pass Rate', `${kpis.passRate}%`, 'trending-up', kpis.passRateChange, kpis.passRate >= 90 ? 'success' : (kpis.passRate >= 70 ? 'warning' : 'error'))}
                    ${this.createKPICard('Failed Rules', kpis.failedCount, 'alert-circle', kpis.failedChange, kpis.failedCount === 0 ? 'success' : 'error')}
                    
                    <!-- Quality Gauge KPI -->
                    <div class="kpi-card">
                        <div class="kpi-header">
                            <span class="kpi-title">QUALITY SCORE</span>
                        </div>
                        <div id="qualityGauge" style="height: 150px; margin-top: 0.5rem;"></div>
                    </div>
                </div>
                
                <!-- Charts Row -->
                <div class="charts-row">
                    <!-- Pass/Fail Doughnut -->
                    <div class="chart-card">
                        <div class="chart-card-header">
                            <h3>Pass/Fail Overview</h3>
                        </div>
                        <div class="chart-container" style="height: 280px;">
                            <canvas id="passFailDoughnutChart"></canvas>
                        </div>
                    </div>
                    
                    <!-- Rule Execution Times -->
                    <div class="chart-card">
                        <div class="chart-card-header">
                            <h3>Rule Execution Times</h3>
                            <button class="btn btn-sm btn-outline" onclick="Dashboard.exportRuleTimesChart()">
                                Export PNG
                            </button>
                        </div>
                        <div class="chart-container" style="height: 280px;">
                            <canvas id="ruleTimesChart"></canvas>
                        </div>
                    </div>
                </div>
            </div>
        `;

        // Create charts
        setTimeout(() => {
            this.createExecutiveCharts(kpis, results.results);
        }, 100);
    },

    /**
     * Create KPI Card HTML
     */
    createKPICard(title, value, icon, change, variant = 'default') {
        const changeHTML = change !== null && change !== undefined ? `
            <span class="kpi-change ${change >= 0 ? 'positive' : 'negative'}">
                ${change >= 0 ? '↑' : '↓'} ${Math.abs(change)}%
            </span>
        ` : '';

        return `
            <div class="kpi-card ${variant}">
                <div class="kpi-header">
                    <span class="kpi-title">${title}</span>
                    <div class="kpi-icon">
                        ${this.getIcon(icon)}
                    </div>
                </div>
                <div class="kpi-value">${value}</div>
                ${changeHTML}
            </div>
        `;
    },

    /**
     * Get icon SVG
     */
    getIcon(name) {
        const icons = {
            'check-circle': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>',
            'trending-up': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline><polyline points="17 6 23 6 23 12"></polyline></svg>',
            'alert-circle': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>',
            'award': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="8" r="7"></circle><polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88"></polyline></svg>'
        };
        return icons[name] || icons['check-circle'];
    },

    /**
     * Calculate KPIs from results
     */
    calculateKPIs(results) {
        const totalChecks = results.length;
        const passedCount = results.filter(r => r.passed).length;
        const failedCount = totalChecks - passedCount;
        const passRate = totalChecks > 0 ? ((passedCount / totalChecks) * 100).toFixed(1) : 0;

        return {
            totalChecks,
            passedCount,
            failedCount,
            passRate: parseFloat(passRate),
            totalChange: null,  // TODO: Calculate from historical data
            passRateChange: null,
            failedChange: null
        };
    },

    /**
     * Get quality grade
     */
    getQualityGrade(passRate) {
        if (passRate >= 95) return 'A+';
        if (passRate >= 90) return 'A';
        if (passRate >= 85) return 'B+';
        if (passRate >= 80) return 'B';
        if (passRate >= 70) return 'C';
        if (passRate >= 60) return 'D';
        return 'F';
    },

    /**
     * Get quality color
     */
    getQualityColor(passRate) {
        if (passRate >= 90) return 'success';
        if (passRate >= 70) return 'warning';
        return 'error';
    },

    /**
     * Create executive dashboard charts
     */
    createExecutiveCharts(kpis, results) {
        // Quality Gauge
        QCCharts.createQualityGauge('qualityGauge', kpis.passRate);

        // Pass/Fail Doughnut
        QCCharts.createPassFailDoughnut('passFailDoughnutChart', kpis.passedCount, kpis.failedCount);

        // Rule Execution Times (mock data for now - will be real when we add timing)
        const ruleLabels = results.map(r => r.rule_name || 'Unknown');
        const ruleTimes = results.map(() => Math.random() * 2 + 0.1); // Mock data

        QCCharts.createHorizontalBarChart('ruleTimesChart', ruleLabels, ruleTimes, null);
    },

    /**
     * Export rule times chart
     */
    exportRuleTimesChart() {
        QCCharts.exportChartPNG('ruleTimesChart', 'rule_execution_times.png');
        App.showToast('Chart exported successfully', 'success');
    },

    /**
     * Switch dashboard view
     */
    switchView(view) {
        this.state.currentView = view;
        // TODO: Implement view switching
    }
};

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    Dashboard.init();
});
