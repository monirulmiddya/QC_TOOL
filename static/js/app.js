/**
 * QC Tool - Main Application JavaScript
 * Handles state management, navigation, and API communication
 */

// Application State
const App = {
    state: {
        currentView: 'data',
        sessions: [],
        activeSession: null,
        qcResults: null,
        resultId: null,
        availableRules: [],
        activeRules: []
    },

    // API Base URL
    API_BASE: '',

    // Initialize application
    init() {
        this.setupNavigation();
        this.setupModals();
        this.setupExport();
        this.loadAvailableRules();
        console.log('QC Tool initialized');
    },

    // Navigation
    setupNavigation() {
        document.querySelectorAll('[data-view]').forEach(btn => {
            btn.addEventListener('click', () => {
                const view = btn.dataset.view;
                this.switchView(view);
            });
        });
    },

    switchView(viewName) {
        // Update nav buttons
        document.querySelectorAll('.nav-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.view === viewName);
        });

        // Update views
        document.querySelectorAll('.view').forEach(view => {
            view.classList.toggle('active', view.id === `${viewName}View`);
        });

        this.state.currentView = viewName;

        // Trigger view-specific updates
        if (viewName === 'qc') {
            this.updateQCView();
        } else if (viewName === 'compare') {
            this.updateCompareView();
        } else if (viewName === 'results') {
            this.updateResultsView();
        }
    },

    // Update QC view based on data availability
    updateQCView() {
        const hasData = this.state.sessions.length > 0;
        document.getElementById('noDataMessage').classList.toggle('hidden', hasData);
        document.getElementById('qcContainer').classList.toggle('hidden', !hasData);
    },

    // Update Compare view with available sessions
    updateCompareView() {
        const sourceSelect = document.getElementById('sourceDataset');
        const targetSelect = document.getElementById('targetDataset');

        const options = this.state.sessions.map(s =>
            `<option value="${s.session_id}">${s.source}: ${s.row_count} rows</option>`
        ).join('');

        const defaultOption = '<option value="">Select dataset...</option>';
        sourceSelect.innerHTML = defaultOption + options;
        targetSelect.innerHTML = defaultOption + options;

        document.getElementById('runCompareBtn').disabled = this.state.sessions.length < 2;
    },

    // Update Results view
    updateResultsView() {
        const hasResults = this.state.qcResults !== null;
        document.getElementById('noResultsMessage').classList.toggle('hidden', hasResults);
        document.getElementById('resultsContainer').classList.toggle('hidden', !hasResults);
        document.getElementById('exportBtn').disabled = !hasResults;
    },

    // Modal handling
    setupModals() {
        document.querySelectorAll('.modal').forEach(modal => {
            // Close on background click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.classList.add('hidden');
                }
            });

            // Close buttons
            modal.querySelectorAll('.modal-close, .modal-cancel').forEach(btn => {
                btn.addEventListener('click', () => {
                    modal.classList.add('hidden');
                });
            });
        });
    },

    showModal(modalId) {
        document.getElementById(modalId).classList.remove('hidden');
    },

    hideModal(modalId) {
        document.getElementById(modalId).classList.add('hidden');
    },

    // Export functionality
    setupExport() {
        document.getElementById('exportBtn').addEventListener('click', () => {
            if (this.state.resultId) {
                this.showModal('exportModal');
            }
        });

        document.getElementById('confirmExportBtn').addEventListener('click', () => {
            this.exportResults();
        });
    },

    async exportResults() {
        const format = document.querySelector('input[name="exportFormat"]:checked').value;
        const includeFailedRows = document.getElementById('includeFailedRows').checked;

        this.showLoading('Exporting results...');

        try {
            const response = await fetch(`${this.API_BASE}/api/export/${format}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    result_id: this.state.resultId,
                    include_failed_rows: includeFailedRows
                })
            });

            if (!response.ok) throw new Error('Export failed');

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `qc_results.${format === 'excel' ? 'xlsx' : 'csv'}`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            a.remove();

            this.hideModal('exportModal');
            this.showToast('Export completed successfully', 'success');
        } catch (error) {
            this.showToast(`Export failed: ${error.message}`, 'error');
        } finally {
            this.hideLoading();
        }
    },

    // Load available QC rules
    async loadAvailableRules() {
        try {
            const response = await fetch(`${this.API_BASE}/api/qc/rules`);
            const data = await response.json();

            if (data.success) {
                this.state.availableRules = data.rules;
                QCRules.renderRulesList(data.rules);
            }
        } catch (error) {
            console.error('Failed to load rules:', error);
        }
    },

    // Add session to state
    addSession(sessionData) {
        this.state.sessions.push(sessionData);
        this.state.activeSession = sessionData.session_id;
        this.updateQCView();
        this.updateCompareView();
    },

    // Set QC results
    setResults(results, resultId) {
        this.state.qcResults = results;
        this.state.resultId = resultId;
        Results.render(results);
        this.switchView('results');
    },

    // Loading overlay
    showLoading(message = 'Loading...') {
        document.getElementById('loadingMessage').textContent = message;
        document.getElementById('loadingOverlay').classList.remove('hidden');
    },

    hideLoading() {
        document.getElementById('loadingOverlay').classList.add('hidden');
    },

    // Toast notifications
    showToast(message, type = 'info') {
        const container = document.getElementById('toastContainer');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;
        container.appendChild(toast);

        setTimeout(() => {
            toast.style.animation = 'slideIn 0.3s ease reverse';
            setTimeout(() => toast.remove(), 300);
        }, 4000);
    },

    // API helper
    async apiRequest(endpoint, options = {}) {
        const response = await fetch(`${this.API_BASE}${endpoint}`, {
            headers: { 'Content-Type': 'application/json' },
            ...options
        });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || 'Request failed');
        }

        return data;
    }
};

// Initialize on DOM load
document.addEventListener('DOMContentLoaded', () => {
    App.init();
    DataSource.init();
    QCRules.init();
    Results.init();
});
