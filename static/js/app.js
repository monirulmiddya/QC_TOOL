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

        // Sync sessions before updating views
        if (typeof DataSource !== 'undefined' && DataSource.loadSessions) {
            DataSource.loadSessions().then(() => {
                this._triggerViewUpdate(viewName);
            });
        } else {
            this._triggerViewUpdate(viewName);
        }
    },

    _triggerViewUpdate(viewName) {
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
    async updateQCView() {
        // Fetch sessions from API to ensure we have latest data
        try {
            const response = await fetch(`${this.API_BASE}/api/data/sessions`);
            const data = await response.json();
            if (data.success && data.sessions) {
                this.state.sessions = data.sessions;
            }
        } catch (e) {
            console.error('Failed to fetch sessions:', e);
        }

        const hasData = this.state.sessions.length > 0;
        const noDataMsg = document.getElementById('noDataMessage');
        const qcContainer = document.getElementById('qcContainer');

        // Force hide/show with explicit classList operations
        if (noDataMsg) {
            if (hasData) {
                noDataMsg.classList.add('hidden');
            } else {
                noDataMsg.classList.remove('hidden');
            }
        }
        if (qcContainer) {
            if (hasData) {
                qcContainer.classList.remove('hidden');
            } else {
                qcContainer.classList.add('hidden');
            }
        }

        // Populate source dropdown
        if (hasData) {
            const select = document.getElementById('qcSourceSelect');
            if (!select) return;

            // Auto-select first session if none selected
            if (!this.state.activeSession && this.state.sessions.length > 0) {
                this.state.activeSession = this.state.sessions[0].session_id;
            }

            select.innerHTML = '<option value="">Select source...</option>' +
                this.state.sessions.map(s =>
                    `<option value="${s.session_id}" ${s.session_id === this.state.activeSession ? 'selected' : ''}>
                        ${s.source_name || s.source} (${s.row_count.toLocaleString()} rows)
                    </option>`
                ).join('');

            // Show info for selected source
            this.updateSelectedSourceInfo();

            // Add change listener if not already added
            if (!select._hasListener) {
                select.addEventListener('change', () => {
                    this.state.activeSession = select.value;
                    this.updateSelectedSourceInfo();
                    QCRules.updateColumnsFromSession();
                });
                select._hasListener = true;
            }
        }
    },

    // Update selected source info display
    updateSelectedSourceInfo() {
        const infoDiv = document.getElementById('selectedSourceInfo');
        const session = this.state.sessions.find(s => s.session_id === this.state.activeSession);

        if (!session) {
            infoDiv.innerHTML = '';
            return;
        }

        infoDiv.innerHTML = `
            <span class="info-item"><span class="info-label">Rows:</span> ${session.row_count.toLocaleString()}</span>
            <span class="info-item"><span class="info-label">Columns:</span> ${session.column_count || session.columns?.length || 0}</span>
            <span class="info-item"><span class="info-label">Type:</span> ${session.source}</span>
        `;
    },

    // Update Compare view with available sessions
    async updateCompareView() {
        // Fetch sessions from API to ensure we have latest data
        try {
            const response = await fetch(`${this.API_BASE}/api/data/sessions`);
            const data = await response.json();
            if (data.success && data.sessions) {
                this.state.sessions = data.sessions;
            }
        } catch (e) {
            console.error('Failed to fetch sessions:', e);
        }

        const hasData = this.state.sessions.length > 0;
        const noDataMsg = document.getElementById('noCompareDataMessage');
        const container = document.getElementById('compareContainer');

        // Force hide/show with explicit classList operations
        if (noDataMsg) {
            if (hasData) {
                noDataMsg.classList.add('hidden');
            } else {
                noDataMsg.classList.remove('hidden');
            }
        }
        if (container) {
            if (hasData) {
                container.classList.remove('hidden');
            } else {
                container.classList.add('hidden');
            }
        }

        if (!hasData) return;

        // Render source checkbox cards
        const sourcesList = document.getElementById('compareSourcesList');
        if (sourcesList) {
            sourcesList.innerHTML = this.state.sessions.map(s => `
                <label class="source-checkbox-card" data-session-id="${s.session_id}">
                    <input type="checkbox" class="compare-source-checkbox" value="${s.session_id}">
                    <div class="source-checkbox-info">
                        <div class="source-checkbox-name">${s.source_name || s.source}</div>
                        <div class="source-checkbox-meta">${s.row_count.toLocaleString()} rows · ${s.column_count || s.columns?.length || 0} columns</div>
                    </div>
                </label>
            `).join('');

            // Add checkbox change listeners
            sourcesList.querySelectorAll('.compare-source-checkbox').forEach(cb => {
                cb.addEventListener('change', () => {
                    const card = cb.closest('.source-checkbox-card');
                    card.classList.toggle('selected', cb.checked);
                    this.updateCompareColumns();
                    this.updateCompareButtonState();
                });
            });
        }

        // Initial column population
        this.updateCompareColumns();
        this.updateCompareButtonState();

        // Update formula calculator source dropdowns
        if (typeof QCRules !== 'undefined' && QCRules.updateFormulaSources) {
            QCRules.updateFormulaSources();
        }
    },

    // Update column selectors based on selected sources
    updateCompareColumns() {
        const checkboxes = document.querySelectorAll('.compare-source-checkbox:checked');
        const keySelect = document.getElementById('compareKeyColumns');
        const valueSelect = document.getElementById('compareValueColumns');

        if (!keySelect || !valueSelect) return;

        // Get common columns from all selected sources
        let commonColumns = null;
        this.state.allSourceColumns = {};  // Store all columns per source

        checkboxes.forEach(cb => {
            const session = this.state.sessions.find(s => s.session_id === cb.value);
            if (session && session.columns) {
                this.state.allSourceColumns[session.source_name || session.source] = session.columns;
                if (commonColumns === null) {
                    commonColumns = new Set(session.columns);
                } else {
                    commonColumns = new Set([...commonColumns].filter(c => session.columns.includes(c)));
                }
            }
        });

        const columns = commonColumns ? Array.from(commonColumns) : [];

        const optionsHtml = columns.map(col => `<option value="${col}">${col}</option>`).join('');
        keySelect.innerHTML = optionsHtml;
        valueSelect.innerHTML = optionsHtml;

        // Populate aggregation group by columns
        const aggGroupBy = document.getElementById('aggGroupBy');
        if (aggGroupBy) aggGroupBy.innerHTML = optionsHtml;

        // Setup column mapping, aggregation toggle, and templates
        this.setupColumnMapping();
        this.setupAggregationToggle();
        this.setupTemplates();
    },

    // Setup aggregation toggle and dynamic rows
    setupAggregationToggle() {
        const enableCheckbox = document.getElementById('enableAggregation');
        const options = document.getElementById('aggregationOptions');
        const addBtn = document.getElementById('addAggregationBtn');

        if (!enableCheckbox || !options) return;

        if (!enableCheckbox._hasListener) {
            enableCheckbox.addEventListener('change', () => {
                options.classList.toggle('hidden', !enableCheckbox.checked);
                // Add initial row if empty
                if (enableCheckbox.checked) {
                    const list = document.getElementById('aggregationList');
                    if (list && list.children.length === 0) {
                        this.addAggregationRow();
                    }
                }
            });
            enableCheckbox._hasListener = true;
        }

        if (addBtn && !addBtn._hasListener) {
            addBtn.addEventListener('click', () => this.addAggregationRow());
            addBtn._hasListener = true;
        }
    },

    // Add aggregation row with column + function selectors
    addAggregationRow() {
        const container = document.getElementById('aggregationList');
        if (!container) return;

        // Get all columns from selected sources
        const checkboxes = document.querySelectorAll('.compare-source-checkbox:checked');
        let columns = [];
        checkboxes.forEach(cb => {
            const session = this.state.sessions.find(s => s.session_id === cb.value);
            if (session && session.columns) {
                columns = [...new Set([...columns, ...session.columns])];
            }
        });

        if (columns.length === 0) {
            this.showToast('Select sources first to see available columns', 'warning');
            return;
        }

        const rowId = `agg_row_${Date.now()}`;
        const row = document.createElement('div');
        row.className = 'aggregation-row';
        row.id = rowId;
        row.innerHTML = `
            <select class="form-select agg-column-select" style="flex: 2;">
                <option value="">Select column...</option>
                ${columns.map(c => `<option value="${c}">${c}</option>`).join('')}
            </select>
            <select class="form-select agg-function-select" style="flex: 1;">
                <option value="sum">SUM</option>
                <option value="count">COUNT</option>
                <option value="avg">AVG</option>
                <option value="min">MIN</option>
                <option value="max">MAX</option>
            </select>
            <button type="button" class="btn btn-danger btn-sm" onclick="document.getElementById('${rowId}').remove()" title="Remove">✕</button>
        `;
        container.appendChild(row);
    },

    // Get aggregation configs (array of {column, function}) plus group_by
    getAggregationConfigs() {
        const enabled = document.getElementById('enableAggregation')?.checked;
        if (!enabled) return null;

        const container = document.getElementById('aggregationList');
        if (!container) return null;

        // Get group by columns
        const groupBySelect = document.getElementById('aggGroupBy');
        const groupBy = groupBySelect ? Array.from(groupBySelect.selectedOptions).map(o => o.value) : [];

        const configs = [];
        container.querySelectorAll('.aggregation-row').forEach(row => {
            const column = row.querySelector('.agg-column-select')?.value;
            const func = row.querySelector('.agg-function-select')?.value;
            if (column && func) {
                configs.push({ column, function: func });
            }
        });

        if (configs.length === 0) return null;

        return {
            aggregations: configs,
            group_by: groupBy.length > 0 ? groupBy : null
        };
    },

    // Setup QC templates
    setupTemplates() {
        const saveBtn = document.getElementById('saveTemplateBtn');
        const loadSelect = document.getElementById('loadTemplate');

        if (saveBtn && !saveBtn._hasListener) {
            saveBtn.addEventListener('click', () => this.saveTemplate());
            saveBtn._hasListener = true;
        }

        if (loadSelect && !loadSelect._hasListener) {
            loadSelect.addEventListener('change', () => this.loadTemplate(loadSelect.value));
            loadSelect._hasListener = true;
            this.refreshTemplateList();
        }
    },

    // Save current config as template
    saveTemplate() {
        const name = document.getElementById('templateName')?.value.trim();
        if (!name) {
            this.showToast('Enter a template name', 'warning');
            return;
        }

        const config = {
            joinType: document.getElementById('compareJoinType')?.value,
            tolerance: document.getElementById('compareTolerance')?.value,
            toleranceType: document.getElementById('compareToleranceType')?.value,
            dateTolerance: document.getElementById('compareDateTolerance')?.value,
            ignoreCase: document.getElementById('compareIgnoreCase')?.checked,
            ignoreWhitespace: document.getElementById('compareIgnoreWhitespace')?.checked,
            nullEqualsNull: document.getElementById('compareNullEqualsNull')?.checked,
            enableAggregation: document.getElementById('enableAggregation')?.checked,
            aggFunction: document.getElementById('aggFunction')?.value,
            enableFuzzyMatch: document.getElementById('enableFuzzyMatch')?.checked,
            fuzzyThreshold: document.getElementById('fuzzyThreshold')?.value,
            transformations: Array.from(document.getElementById('transformations')?.selectedOptions || []).map(o => o.value),
            showDuplicates: document.getElementById('showDuplicates')?.checked,
            showUnique: document.getElementById('showUnique')?.checked,
            showNotMatched: document.getElementById('showNotMatched')?.checked
        };

        const templates = JSON.parse(localStorage.getItem('qc_templates') || '{}');
        templates[name] = config;
        localStorage.setItem('qc_templates', JSON.stringify(templates));

        this.refreshTemplateList();
        document.getElementById('templateName').value = '';
        this.showToast(`Template "${name}" saved`, 'success');
    },

    // Load a template
    loadTemplate(name) {
        if (!name) return;

        const templates = JSON.parse(localStorage.getItem('qc_templates') || '{}');
        const config = templates[name];
        if (!config) return;

        // Apply config
        if (config.joinType) document.getElementById('compareJoinType').value = config.joinType;
        if (config.tolerance) document.getElementById('compareTolerance').value = config.tolerance;
        if (config.toleranceType) document.getElementById('compareToleranceType').value = config.toleranceType;
        if (config.dateTolerance) document.getElementById('compareDateTolerance').value = config.dateTolerance;
        document.getElementById('compareIgnoreCase').checked = config.ignoreCase || false;
        document.getElementById('compareIgnoreWhitespace').checked = config.ignoreWhitespace || false;
        document.getElementById('compareNullEqualsNull').checked = config.nullEqualsNull !== false;
        document.getElementById('enableAggregation').checked = config.enableAggregation || false;
        if (config.aggFunction) document.getElementById('aggFunction').value = config.aggFunction;
        document.getElementById('enableFuzzyMatch').checked = config.enableFuzzyMatch || false;
        if (config.fuzzyThreshold) document.getElementById('fuzzyThreshold').value = config.fuzzyThreshold;
        document.getElementById('showDuplicates').checked = config.showDuplicates !== false;
        document.getElementById('showUnique').checked = config.showUnique !== false;
        document.getElementById('showNotMatched').checked = config.showNotMatched !== false;

        // Handle transformation multi-select
        if (config.transformations) {
            const select = document.getElementById('transformations');
            Array.from(select.options).forEach(opt => {
                opt.selected = config.transformations.includes(opt.value);
            });
        }

        // Toggle aggregation options visibility
        const aggOptions = document.getElementById('aggregationOptions');
        if (aggOptions) aggOptions.classList.toggle('hidden', !config.enableAggregation);

        this.showToast(`Template "${name}" loaded`, 'success');
    },

    // Refresh template dropdown
    refreshTemplateList() {
        const select = document.getElementById('loadTemplate');
        if (!select) return;

        const templates = JSON.parse(localStorage.getItem('qc_templates') || '{}');
        const names = Object.keys(templates);

        select.innerHTML = '<option value="">Select saved template...</option>' +
            names.map(n => `<option value="${n}">${n}</option>`).join('');
    },

    // Setup column mapping UI
    setupColumnMapping() {
        const addBtn = document.getElementById('addColumnMapping');
        if (!addBtn) return;

        if (!addBtn._hasListener) {
            addBtn.addEventListener('click', () => this.addColumnMappingRow());
            addBtn._hasListener = true;
        }
    },

    // Add a column mapping row
    addColumnMappingRow() {
        const container = document.getElementById('columnMappingList');
        if (!container) return;

        const sources = Object.keys(this.state.allSourceColumns || {});
        if (sources.length < 2) {
            this.showToast('Select at least 2 sources first', 'warning');
            return;
        }

        const rowId = `mapping_${Date.now()}`;
        const row = document.createElement('div');
        row.className = 'column-mapping-row';
        row.id = rowId;

        // Create select for each source
        const selects = sources.map((source, idx) => {
            const cols = this.state.allSourceColumns[source] || [];
            const options = cols.map(c => `<option value="${c}">${c}</option>`).join('');
            return `
                <select class="form-select mapping-source-${idx}" data-source="${source}">
                    <option value="">${source}</option>
                    ${options}
                </select>
            `;
        }).join('<span class="column-mapping-arrow">↔</span>');

        row.innerHTML = `
            ${selects}
            <button type="button" class="column-mapping-remove" onclick="document.getElementById('${rowId}').remove()">✕</button>
        `;

        container.appendChild(row);
    },

    // Get all column mappings
    getColumnMappings() {
        const container = document.getElementById('columnMappingList');
        if (!container) return [];

        const mappings = [];
        container.querySelectorAll('.column-mapping-row').forEach(row => {
            const selects = row.querySelectorAll('select');
            const mapping = {};
            selects.forEach(sel => {
                if (sel.value) {
                    mapping[sel.dataset.source] = sel.value;
                }
            });
            if (Object.keys(mapping).length >= 2) {
                mappings.push(mapping);
            }
        });
        return mappings;
    },

    // Update compare button state
    updateCompareButtonState() {
        const checkboxes = document.querySelectorAll('.compare-source-checkbox:checked');
        const runBtn = document.getElementById('runCompareBtn');
        if (runBtn) {
            runBtn.disabled = checkboxes.length < 2;
        }
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
