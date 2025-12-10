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
        this.currentRuleConfig = { rule_id: rule.id, config: {} };

        document.getElementById('ruleConfigTitle').textContent = `Configure: ${rule.name}`;

        const body = document.getElementById('ruleConfigBody');
        const schema = rule.config_schema.properties || {};
        const columns = App.state.sessions.length > 0
            ? App.state.sessions[App.state.sessions.length - 1].columns
            : [];

        body.innerHTML = this.generateFormFields(schema, columns);
        App.showModal('ruleConfigModal');
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
        if (this.activeRules.length === 0 || !App.state.activeSession) {
            App.showToast('Please add at least one rule and load data', 'warning');
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
        document.getElementById('runCompareBtn').addEventListener('click', () => {
            this.runComparison();
        });

        // Enable/disable button based on selection
        ['sourceDataset', 'targetDataset'].forEach(id => {
            document.getElementById(id).addEventListener('change', () => {
                const source = document.getElementById('sourceDataset').value;
                const target = document.getElementById('targetDataset').value;
                document.getElementById('runCompareBtn').disabled = !source || !target || source === target;
            });
        });
    },

    async runComparison() {
        const sourceId = document.getElementById('sourceDataset').value;
        const targetId = document.getElementById('targetDataset').value;
        const keyColumns = document.getElementById('keyColumns').value
            .split(',')
            .map(s => s.trim())
            .filter(s => s);
        const tolerance = parseFloat(document.getElementById('tolerance').value) || 0;
        const ignoreCase = document.getElementById('ignoreCase').checked;
        const ignoreWhitespace = document.getElementById('ignoreWhitespace').checked;

        if (!sourceId || !targetId) {
            App.showToast('Please select both datasets', 'warning');
            return;
        }

        App.showLoading('Comparing datasets...');

        try {
            const response = await fetch(`${App.API_BASE}/api/qc/compare`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    source_session_id: sourceId,
                    target_session_id: targetId,
                    key_columns: keyColumns,
                    tolerance: tolerance,
                    ignore_case: ignoreCase,
                    ignore_whitespace: ignoreWhitespace
                })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Comparison failed');
            }

            // Format as result-like structure
            const comparisonResult = [{
                rule_name: 'Dataset Comparison',
                passed: data.match,
                message: data.message,
                details: data.column_differences,
                statistics: data.summary
            }];

            App.setResults(comparisonResult, data.result_id);

            if (data.match) {
                App.showToast('Datasets match!', 'success');
            } else {
                App.showToast('Differences found between datasets', 'warning');
            }

        } catch (error) {
            App.showToast(`Comparison failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    }
};
