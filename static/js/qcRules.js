/**
 * QC Tool - QC Rules Module
 * Handles QC rule configuration and execution
 */

const QCRules = {
    activeRules: [],
    currentRuleConfig: null,

    init() {
        this.setupRuleModal();
        this.setupRunButton();
        this.setupCompare();
        this.setupFormulaCalculator();
    },

    renderRulesList(rules) {
        const container = document.getElementById('rulesList');

        container.innerHTML = rules.map(rule => `
            <div class="rule-item" data-rule-id="${rule.id}">
                <div class="rule-item-title">${rule.name}</div>
                <div class="rule-item-desc">${rule.description}</div>
            </div>
        `).join('');

        // Add click handlers
        container.querySelectorAll('.rule-item').forEach(item => {
            item.addEventListener('click', () => {
                const ruleId = item.dataset.ruleId;
                const rule = rules.find(r => r.id === ruleId);
                this.showRuleConfig(rule);
            });
        });
    },

    showRuleConfig(rule) {
        // Check if a source is selected
        if (!App.state.activeSession) {
            App.showToast('Please select a data source first', 'warning');
            return;
        }

        this.currentRuleConfig = { rule_id: rule.id, config: {} };

        document.getElementById('ruleConfigTitle').textContent = `Configure: ${rule.name}`;

        const body = document.getElementById('ruleConfigBody');
        const schema = rule.config_schema.properties || {};

        // Get columns from the selected session
        const session = App.state.sessions.find(s => s.session_id === App.state.activeSession);
        const columns = session?.columns || [];

        body.innerHTML = this.generateFormFields(schema, columns);
        App.showModal('ruleConfigModal');
    },

    // Method called when session changes to update column lists
    updateColumnsFromSession() {
        // This is called when the source dropdown changes
        // Could be used to refresh any open rule configuration
        console.log('Session changed to:', App.state.activeSession);
    },

    generateFormFields(schema, columns) {
        let html = '';

        for (const [key, prop] of Object.entries(schema)) {
            const label = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            const required = prop.required ? '<span style="color:var(--error)">*</span>' : '';

            html += `<div class="form-group" style="margin-bottom: 1rem;">`;
            html += `<label>${label} ${required}</label>`;

            if (prop.type === 'array' && (key.includes('column') || key === 'group_by')) {
                // Column multi-select
                html += `<select id="config_${key}" multiple class="form-select" style="min-height: 100px;">`;
                columns.forEach(col => {
                    html += `<option value="${col}">${col}</option>`;
                });
                html += `</select>`;
                html += `<small style="color:var(--text-muted)">Hold Ctrl/Cmd to select multiple</small>`;
            } else if (key === 'column' || key.includes('column')) {
                // Single column select
                html += `<select id="config_${key}" class="form-select">`;
                html += `<option value="">Select column...</option>`;
                columns.forEach(col => {
                    html += `<option value="${col}">${col}</option>`;
                });
                html += `</select>`;
            } else if (prop.enum) {
                // Enum select
                html += `<select id="config_${key}" class="form-select">`;
                prop.enum.forEach(opt => {
                    const selected = opt === prop.default ? 'selected' : '';
                    html += `<option value="${opt}" ${selected}>${opt}</option>`;
                });
                html += `</select>`;
            } else if (prop.type === 'boolean') {
                // Checkbox
                const checked = prop.default ? 'checked' : '';
                html += `<label class="checkbox-label">
                    <input type="checkbox" id="config_${key}" ${checked}>
                    ${prop.description || ''}
                </label>`;
            } else if (prop.type === 'number' || prop.type === 'integer') {
                // Number input
                html += `<input type="number" id="config_${key}" 
                    value="${prop.default || ''}" 
                    ${prop.minimum !== undefined ? `min="${prop.minimum}"` : ''}
                    ${prop.maximum !== undefined ? `max="${prop.maximum}"` : ''}
                    step="${prop.type === 'integer' ? '1' : 'any'}"
                    placeholder="${prop.description || ''}">`;
            } else {
                // Text input
                html += `<input type="text" id="config_${key}" 
                    value="${prop.default || ''}"
                    placeholder="${prop.description || ''}">`;
            }

            if (prop.description && prop.type !== 'boolean') {
                html += `<small style="color:var(--text-muted);display:block;margin-top:0.25rem">${prop.description}</small>`;
            }

            html += `</div>`;
        }

        return html;
    },

    setupRuleModal() {
        document.getElementById('addRuleBtn').addEventListener('click', () => {
            this.addRuleFromModal();
        });
    },

    addRuleFromModal() {
        if (!this.currentRuleConfig) return;

        const rule = App.state.availableRules.find(r => r.id === this.currentRuleConfig.rule_id);
        const schema = rule.config_schema.properties || {};
        const config = {};

        // Gather form values
        for (const key of Object.keys(schema)) {
            const input = document.getElementById(`config_${key}`);
            if (!input) continue;

            if (input.type === 'checkbox') {
                config[key] = input.checked;
            } else if (input.multiple) {
                config[key] = Array.from(input.selectedOptions).map(o => o.value);
            } else if (input.type === 'number') {
                const val = parseFloat(input.value);
                if (!isNaN(val)) config[key] = val;
            } else if (input.value) {
                config[key] = input.value;
            }
        }

        // Validate required fields
        const required = rule.config_schema.required || [];
        for (const field of required) {
            if (!config[field] || (Array.isArray(config[field]) && config[field].length === 0)) {
                App.showToast(`${field} is required`, 'error');
                return;
            }
        }

        this.activeRules.push({
            rule_id: this.currentRuleConfig.rule_id,
            rule_name: rule.name,
            config: config
        });

        App.state.activeRules = this.activeRules;
        this.renderActiveRules();
        App.hideModal('ruleConfigModal');
    },

    renderActiveRules() {
        const container = document.getElementById('activeRulesList');
        const count = document.getElementById('activeRuleCount');
        const runBtn = document.getElementById('runQcBtn');

        count.textContent = `(${this.activeRules.length})`;
        runBtn.disabled = this.activeRules.length === 0;

        if (this.activeRules.length === 0) {
            container.innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:2rem">No rules configured. Click a rule on the left to add it.</p>';
            return;
        }

        container.innerHTML = this.activeRules.map((rule, index) => {
            const configSummary = Object.entries(rule.config)
                .map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(', ') : v}`)
                .join(' | ');

            return `
                <div class="active-rule-card">
                    <div class="active-rule-header">
                        <span class="active-rule-title">${rule.rule_name}</span>
                        <button class="active-rule-remove" onclick="QCRules.removeRule(${index})">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    </div>
                    <div class="active-rule-config">${configSummary || 'Using defaults'}</div>
                </div>
            `;
        }).join('');
    },

    removeRule(index) {
        this.activeRules.splice(index, 1);
        App.state.activeRules = this.activeRules;
        this.renderActiveRules();
    },

    setupRunButton() {
        document.getElementById('runQcBtn').addEventListener('click', () => {
            this.runQCChecks();
        });
    },

    async runQCChecks() {
        if (this.activeRules.length === 0) {
            App.showToast('Please add at least one rule', 'warning');
            return;
        }

        if (!App.state.activeSession) {
            App.showToast('Please select a data source', 'warning');
            return;
        }

        App.showLoading('Running QC checks...');

        try {
            const response = await fetch(`${App.API_BASE}/api/qc/run`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    session_id: App.state.activeSession,
                    rules: this.activeRules.map(r => ({
                        rule_id: r.rule_id,
                        config: r.config
                    }))
                })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'QC check failed');
            }

            App.setResults(data.results, data.result_id);

            if (data.all_passed) {
                App.showToast('All QC checks passed!', 'success');
            } else {
                App.showToast(`${data.failed_count} of ${data.total_rules} checks failed`, 'warning');
            }

        } catch (error) {
            App.showToast(`QC check failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    },

    setupCompare() {
        const runBtn = document.getElementById('runCompareBtn');
        if (runBtn) {
            runBtn.addEventListener('click', () => {
                this.runComparison();
            });
        }
    },

    async runComparison() {
        // Get selected sources
        const selectedCheckboxes = document.querySelectorAll('.compare-source-checkbox:checked');
        const sessionIds = Array.from(selectedCheckboxes).map(cb => cb.value);

        if (sessionIds.length < 2) {
            App.showToast('Please select at least 2 sources to compare', 'warning');
            return;
        }

        // Get column selections
        const keyColumnsSelect = document.getElementById('compareKeyColumns');
        const valueColumnsSelect = document.getElementById('compareValueColumns');

        const keyColumns = Array.from(keyColumnsSelect.selectedOptions).map(o => o.value);
        const valueColumns = Array.from(valueColumnsSelect.selectedOptions).map(o => o.value);

        // Get JOIN type
        const joinType = document.getElementById('compareJoinType')?.value || 'full';

        // Get tolerance options
        const tolerance = parseFloat(document.getElementById('compareTolerance')?.value) || 0;
        const toleranceType = document.getElementById('compareToleranceType')?.value || 'absolute';
        const dateTolerance = parseFloat(document.getElementById('compareDateTolerance')?.value) || 0;
        const dateToleranceUnit = document.getElementById('compareDateToleranceUnit')?.value || 'days';

        // Get string/null handling
        const ignoreCase = document.getElementById('compareIgnoreCase')?.checked || false;
        const ignoreWhitespace = document.getElementById('compareIgnoreWhitespace')?.checked || false;
        const nullEqualsNull = document.getElementById('compareNullEqualsNull')?.checked ?? true;

        // Get analysis options
        const showDuplicates = document.getElementById('showDuplicates')?.checked ?? true;
        const showUnique = document.getElementById('showUnique')?.checked ?? true;
        const showNotMatched = document.getElementById('showNotMatched')?.checked ?? true;

        // Get column mappings
        const columnMappings = App.getColumnMappings ? App.getColumnMappings() : [];

        // Get fuzzy matching options
        const enableFuzzyMatch = document.getElementById('enableFuzzyMatch')?.checked || false;
        const fuzzyThreshold = parseFloat(document.getElementById('fuzzyThreshold')?.value) || 80;

        // Get transformations
        const transformSelect = document.getElementById('transformations');
        const transformations = transformSelect ? Array.from(transformSelect.selectedOptions).map(o => o.value) : [];

        if (keyColumns.length === 0) {
            App.showToast('Please select at least one key column for matching', 'warning');
            return;
        }

        App.showLoading('Comparing sources...');

        try {
            const response = await fetch(`${App.API_BASE}/api/qc/compare`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    session_ids: sessionIds,
                    key_columns: keyColumns,
                    value_columns: valueColumns.length > 0 ? valueColumns : null,
                    join_type: joinType,
                    column_mappings: columnMappings.length > 0 ? columnMappings : null,
                    tolerance: {
                        numeric: tolerance,
                        numeric_type: toleranceType,
                        date: dateTolerance,
                        date_unit: dateToleranceUnit
                    },
                    options: {
                        ignore_case: ignoreCase,
                        ignore_whitespace: ignoreWhitespace,
                        null_equals_null: nullEqualsNull,
                        fuzzy_match: enableFuzzyMatch,
                        fuzzy_threshold: fuzzyThreshold,
                        transformations: transformations
                    },
                    analysis: {
                        duplicates: showDuplicates,
                        unique: showUnique,
                        not_matched: showNotMatched
                    },
                    aggregation: this.getAggregationConfig()
                })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Comparison failed');
            }

            // Format results for display
            const results = [];

            if (data.duplicates && showDuplicates) {
                results.push({
                    rule_name: '🔄 Duplicates (In Multiple Sources)',
                    passed: data.duplicates.count === 0,
                    message: `Found ${data.duplicates.count} duplicate rows across sources`,
                    statistics: { duplicate_count: data.duplicates.count },
                    failed_rows: data.duplicates.rows?.slice(0, 100),
                    failed_row_count: data.duplicates.count
                });
            }

            if (data.unique && showUnique) {
                for (const [source, info] of Object.entries(data.unique)) {
                    results.push({
                        rule_name: `🔹 Unique to: ${source}`,
                        passed: true,
                        message: `${info.count} rows only in this source`,
                        statistics: { unique_count: info.count },
                        failed_rows: info.rows?.slice(0, 100),
                        failed_row_count: info.count
                    });
                }
            }

            if (data.not_matched && showNotMatched) {
                results.push({
                    rule_name: '❌ Value Differences',
                    passed: data.not_matched.count === 0,
                    message: `Found ${data.not_matched.count} rows with value differences`,
                    statistics: { difference_count: data.not_matched.count },
                    details: data.not_matched.column_differences,
                    failed_rows: data.not_matched.rows?.slice(0, 100),
                    failed_row_count: data.not_matched.count
                });
            }

            // Add aggregation results
            if (data.aggregation) {
                const agg = data.aggregation;
                const hasVariance = agg.variances && agg.variances.length > 0;
                results.push({
                    rule_name: `📊 Aggregation: ${agg.function.toUpperCase()}(${agg.column})`,
                    passed: !hasVariance,
                    message: hasVariance
                        ? `${agg.variances.length} groups exceed variance threshold`
                        : `All ${agg.total_groups} groups within tolerance`,
                    statistics: {
                        total_groups: agg.total_groups,
                        variance_count: agg.variances?.length || 0
                    },
                    details: agg.results,
                    failed_rows: agg.variances
                });
            }

            if (results.length === 0) {
                results.push({
                    rule_name: 'Comparison Results',
                    passed: true,
                    message: 'No analysis options selected',
                    statistics: {}
                });
            }

            App.setResults(results, data.result_id);
            App.showToast('Comparison complete', 'success');

        } catch (error) {
            App.showToast(`Comparison failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    },

    // Get aggregation configuration (now delegates to App for dynamic rows)
    getAggregationConfig() {
        // Use App.getAggregationConfigs() for the new multi-column approach
        if (typeof App !== 'undefined' && App.getAggregationConfigs) {
            const configs = App.getAggregationConfigs();
            if (configs && configs.length > 0) {
                return {
                    enabled: true,
                    aggregations: configs  // Array of {column, function}
                };
            }
        }
        return null;
    },

    // Formula Calculator
    setupFormulaCalculator() {
        // Source dropdowns change -> update column dropdowns
        document.getElementById('formulaSource1')?.addEventListener('change', (e) => {
            this.updateFormulaColumns('formulaColumn1', e.target.value);
        });
        document.getElementById('formulaSource2')?.addEventListener('change', (e) => {
            this.updateFormulaColumns('formulaColumn2', e.target.value);
        });

        // Match by change -> show/hide key columns
        document.getElementById('formulaMatchBy')?.addEventListener('change', (e) => {
            const keyGroup = document.getElementById('formulaKeyColumnGroup');
            if (keyGroup) {
                keyGroup.style.display = e.target.value === 'key' ? 'block' : 'none';
            }
            // Update key columns list with common columns
            if (e.target.value === 'key') {
                this.updateFormulaKeyColumns();
            }
        });

        // Run formula button
        document.getElementById('runFormulaBtn')?.addEventListener('click', () => {
            this.runFormulaCalculation();
        });
    },

    updateFormulaColumns(selectId, sessionId) {
        const select = document.getElementById(selectId);
        if (!select) return;

        select.innerHTML = '<option value="">Select column...</option>';

        if (!sessionId) return;

        const session = App.state.sessions.find(s => s.session_id === sessionId);
        if (session?.columns) {
            session.columns.forEach(col => {
                select.innerHTML += `<option value="${col}">${col}</option>`;
            });
        }
    },

    updateFormulaSources() {
        const source1 = document.getElementById('formulaSource1');
        const source2 = document.getElementById('formulaSource2');

        if (!source1 || !source2) return;

        const options = App.state.sessions.map(s =>
            `<option value="${s.session_id}">${s.source_name}</option>`
        ).join('');

        source1.innerHTML = '<option value="">Select source...</option>' + options;
        source2.innerHTML = '<option value="">Select source...</option>' + options;
    },

    updateFormulaKeyColumns() {
        const source1Id = document.getElementById('formulaSource1')?.value;
        const source2Id = document.getElementById('formulaSource2')?.value;
        const keySelect = document.getElementById('formulaKeyColumns');

        if (!keySelect) return;

        keySelect.innerHTML = '';

        if (!source1Id || !source2Id) return;

        const session1 = App.state.sessions.find(s => s.session_id === source1Id);
        const session2 = App.state.sessions.find(s => s.session_id === source2Id);

        if (!session1 || !session2) return;

        // Find common columns
        const commonCols = session1.columns.filter(c => session2.columns.includes(c));
        commonCols.forEach(col => {
            keySelect.innerHTML += `<option value="${col}">${col}</option>`;
        });
    },

    async runFormulaCalculation() {
        const source1Id = document.getElementById('formulaSource1')?.value;
        const source2Id = document.getElementById('formulaSource2')?.value;
        const column1 = document.getElementById('formulaColumn1')?.value;
        const column2 = document.getElementById('formulaColumn2')?.value;
        const operation = document.getElementById('formulaOperation')?.value || '-';
        const resultName = document.getElementById('formulaResultName')?.value || 'Calculated';
        const matchBy = document.getElementById('formulaMatchBy')?.value || 'index';
        const keyColumnsSelect = document.getElementById('formulaKeyColumns');
        const keyColumns = keyColumnsSelect ? Array.from(keyColumnsSelect.selectedOptions).map(o => o.value) : [];

        // Validation
        if (!source1Id || !source2Id) {
            App.showToast('Please select both sources', 'warning');
            return;
        }
        if (!column1 || !column2) {
            App.showToast('Please select columns from both sources', 'warning');
            return;
        }
        if (matchBy === 'key' && keyColumns.length === 0) {
            App.showToast('Please select key column(s) for matching', 'warning');
            return;
        }

        App.showLoading('Calculating formula...');

        try {
            const response = await fetch(`${App.API_BASE}/api/qc/calculate`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    source1_id: source1Id,
                    source2_id: source2Id,
                    column1: column1,
                    column2: column2,
                    operation: operation,
                    result_name: resultName,
                    match_by: matchBy,
                    key_columns: keyColumns
                })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Calculation failed');
            }

            // Use the stored results format
            const storedResults = [{
                rule_name: `📊 Formula: ${data.formula}`,
                passed: true,
                message: `Calculated ${data.statistics.calculated_rows} values using ${matchBy}-based matching`,
                statistics: data.statistics,
                failed_rows: data.data,
                failed_row_count: data.statistics.total_rows
            }];

            App.setResults(storedResults, data.result_id);
            App.showToast(`Formula calculated: ${data.statistics.calculated_rows} results`, 'success');

        } catch (error) {
            App.showToast(`Calculation failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    }
};
