/**
 * QC Tool - Results Module
 * Handles results display and formatting
 */

const Results = {
    init() {
        // Nothing to initialize yet
    },

    render(results) {
        if (!results || results.length === 0) {
            document.getElementById('noResultsMessage').classList.remove('hidden');
            document.getElementById('resultsContainer').classList.add('hidden');
            return;
        }

        document.getElementById('noResultsMessage').classList.add('hidden');
        document.getElementById('resultsContainer').classList.remove('hidden');

        this.renderSummary(results);
        this.renderDetails(results);
    },

    renderSummary(results) {
        const container = document.getElementById('resultsSummary');

        const totalRules = results.length;
        const passedCount = results.filter(r => r.passed).length;
        const failedCount = totalRules - passedCount;

        container.innerHTML = `
            <div class="summary-card">
                <div class="summary-value">${totalRules}</div>
                <div class="summary-label">Total Checks</div>
            </div>
            <div class="summary-card success">
                <div class="summary-value" style="color: var(--success)">${passedCount}</div>
                <div class="summary-label">Passed</div>
            </div>
            <div class="summary-card ${failedCount > 0 ? 'error' : ''}">
                <div class="summary-value" style="color: ${failedCount > 0 ? 'var(--error)' : 'var(--text-primary)'}">${failedCount}</div>
                <div class="summary-label">Failed</div>
            </div>
            <div class="summary-card">
                <div class="summary-value">${totalRules > 0 ? Math.round((passedCount / totalRules) * 100) : 0}%</div>
                <div class="summary-label">Pass Rate</div>
            </div>
        `;
    },

    renderDetails(results) {
        const container = document.getElementById('resultsDetail');

        container.innerHTML = results.map((result, resultIdx) => {
            const passed = result.passed;
            const stats = result.statistics || {};
            const details = result.details || {};

            let statsHtml = '';
            for (const [key, value] of Object.entries(stats)) {
                if (typeof value === 'object' || value === null) continue;
                const label = key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                statsHtml += `
                    <div class="result-stat">
                        <span class="result-stat-label">${label}</span>
                        <span class="result-stat-value">${this.formatValue(value)}</span>
                    </div>
                `;
            }

            // Render all rows (not just preview) with collapsible section
            let rowsHtml = '';
            if (result.failed_rows && result.failed_rows.length > 0) {
                const columns = Object.keys(result.failed_rows[0]);
                const totalRows = result.failed_row_count || result.failed_rows.length;
                const initialShow = 20; // Show first 20 rows initially
                const hasMore = result.failed_rows.length > initialShow;

                rowsHtml = `
                    <div style="margin-top: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                            <h4 style="font-size: 0.9rem; margin: 0;">
                                Data Rows 
                                <span style="font-weight: normal; color: var(--text-muted)">
                                    (${totalRows} total${totalRows > result.failed_rows.length ? ', showing ' + result.failed_rows.length : ''})
                                </span>
                            </h4>
                            <div style="display: flex; gap: 0.5rem;">
                                ${hasMore ? `
                                    <button class="btn btn-outline btn-sm" onclick="Results.toggleShowAll(${resultIdx})" id="toggleBtn_${resultIdx}">
                                        Show All (${result.failed_rows.length})
                                    </button>
                                ` : ''}
                                <button class="btn btn-secondary btn-sm" onclick="Results.exportSection(${resultIdx})" title="Export this section to Excel">
                                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                        <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"></path>
                                        <polyline points="7 10 12 15 17 10"></polyline>
                                        <line x1="12" y1="15" x2="12" y2="3"></line>
                                    </svg>
                                    Export Excel
                                </button>
                            </div>
                        </div>
                        <div class="table-container" style="max-height: 400px; overflow: auto;" id="tableContainer_${resultIdx}">
                            <table class="data-table">
                                <thead>
                                    <tr>${columns.map(c => `<th>${c}</th>`).join('')}</tr>
                                </thead>
                                <tbody id="tableBody_${resultIdx}">
                                    ${result.failed_rows.slice(0, initialShow).map(row =>
                    `<tr>${columns.map(c => `<td>${row[c] !== null && row[c] !== undefined ? row[c] : '<span style="color:var(--text-muted)">null</span>'}</td>`).join('')}</tr>`
                ).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;

                // Store full data for toggle
                if (!this._resultData) this._resultData = {};
                this._resultData[resultIdx] = {
                    rows: result.failed_rows,
                    columns: columns,
                    initialShow: initialShow,
                    expanded: false
                };
            }

            // Render violations/details if present
            let violationsHtml = '';
            if (details.violations && Object.keys(details.violations).length > 0) {
                violationsHtml = `
                    <div style="margin-top: 1rem;">
                        <h4 style="font-size: 0.9rem; margin-bottom: 0.5rem;">Violations</h4>
                        <div class="result-details">
                            ${Object.entries(details.violations).map(([col, data]) => `
                                <div class="result-stat">
                                    <span class="result-stat-label">${col}</span>
                                    <span class="result-stat-value">${typeof data === 'object' ? `${data.null_count} nulls (${data.null_percentage}%)` : data}</span>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Render aggregation details table if present (not just violations)
            let aggDetailsHtml = '';

            // Check for grouped aggregation table (from Aggregation Check with Group By)
            if (details.grouped_table && details.grouped_table.length > 0) {
                const groupedRows = details.grouped_table;
                const tableCols = Object.keys(groupedRows[0]);
                const totalRows = groupedRows.length;
                const showLimit = 50;

                aggDetailsHtml = `
                    <div style="margin-top: 1rem;">
                        <h4 style="font-size: 0.9rem; margin-bottom: 0.5rem;">
                            Grouped Aggregations (${totalRows} groups)
                            <button class="btn btn-secondary btn-sm" style="margin-left: 0.5rem; font-size: 0.7rem;" 
                                onclick="Results.exportGroupedAgg(${resultIdx})">
                                Export CSV
                            </button>
                        </h4>
                        <div class="table-container" style="max-height: 400px; overflow: auto;">
                            <table class="data-table">
                                <thead><tr>${tableCols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
                                <tbody>
                                    ${groupedRows.slice(0, showLimit).map(row =>
                    `<tr>${tableCols.map(c => {
                        const val = row[c];
                        return `<td>${val !== null && val !== undefined ? (typeof val === 'number' ? val.toLocaleString() : val) : ''}</td>`;
                    }).join('')}</tr>`
                ).join('')}
                                </tbody>
                            </table>
                            ${totalRows > showLimit ? `<p style="color:var(--text-muted); text-align:center; padding:0.5rem;">Showing ${showLimit} of ${totalRows} groups</p>` : ''}
                        </div>
                    </div>
                `;

                // Store for export
                if (!this._groupedAggData) this._groupedAggData = {};
                this._groupedAggData[resultIdx] = groupedRows;
            } else if (Array.isArray(details) && details.length > 0) {
                const aggCols = Object.keys(details[0]);
                aggDetailsHtml = `
                    <div style="margin-top: 1rem;">
                        <h4 style="font-size: 0.9rem; margin-bottom: 0.5rem;">Aggregation Details</h4>
                        <div class="table-container" style="max-height: 300px; overflow: auto;">
                            <table class="data-table">
                                <thead><tr>${aggCols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
                                <tbody>
                                    ${details.slice(0, 50).map(row =>
                    `<tr>${aggCols.map(c => {
                        const val = row[c];
                        if (c === 'exceeds_threshold' && val) {
                            return `<td style="color: var(--error); font-weight: bold;">Yes</td>`;
                        }
                        return `<td>${val !== null && val !== undefined ? (typeof val === 'number' ? val.toLocaleString() : val) : ''}</td>`;
                    }).join('')}</tr>`
                ).join('')}
                                </tbody>
                            </table>
                        </div>
                    </div>
                `;
            }

            return `
                <div class="result-card ${passed ? 'passed' : 'failed'}">
                    <div class="result-header">
                        <span class="result-title">${result.rule_name || 'Check'}</span>
                        <span class="result-badge ${passed ? 'pass' : 'fail'}">${passed ? 'PASS' : 'FAIL'}</span>
                    </div>
                    <p class="result-message">${result.message || ''}</p>
                    ${statsHtml ? `<div class="result-details">${statsHtml}</div>` : ''}
                    ${violationsHtml}
                    ${aggDetailsHtml}
                    ${rowsHtml}
                </div>
            `;
        }).join('');
    },

    // Toggle between showing initial rows and all rows
    toggleShowAll(resultIdx) {
        const data = this._resultData?.[resultIdx];
        if (!data) return;

        const tbody = document.getElementById(`tableBody_${resultIdx}`);
        const btn = document.getElementById(`toggleBtn_${resultIdx}`);

        if (!data.expanded) {
            // Show all rows
            tbody.innerHTML = data.rows.map(row =>
                `<tr>${data.columns.map(c => `<td>${row[c] !== null && row[c] !== undefined ? row[c] : '<span style="color:var(--text-muted)">null</span>'}</td>`).join('')}</tr>`
            ).join('');
            btn.textContent = 'Show Less';
            data.expanded = true;
        } else {
            // Show initial rows
            tbody.innerHTML = data.rows.slice(0, data.initialShow).map(row =>
                `<tr>${data.columns.map(c => `<td>${row[c] !== null && row[c] !== undefined ? row[c] : '<span style="color:var(--text-muted)">null</span>'}</td>`).join('')}</tr>`
            ).join('');
            btn.textContent = `Show All (${data.rows.length})`;
            data.expanded = false;
        }
    },

    formatValue(value) {
        if (typeof value === 'number') {
            if (Number.isInteger(value)) {
                return value.toLocaleString();
            }
            return value.toFixed(2);
        }
        if (typeof value === 'boolean') {
            return value ? 'Yes' : 'No';
        }
        return value;
    },

    // Export a specific section's data to CSV (opens in Excel)
    exportSection(resultIdx) {
        const data = this._resultData?.[resultIdx];
        if (!data || !data.rows || data.rows.length === 0) {
            App.showToast('No data to export', 'warning');
            return;
        }

        const { rows, columns } = data;

        // Build CSV content
        const escapeCSV = (val) => {
            if (val === null || val === undefined) return '';
            const str = String(val);
            if (str.includes(',') || str.includes('"') || str.includes('\n')) {
                return '"' + str.replace(/"/g, '""') + '"';
            }
            return str;
        };

        let csv = columns.map(escapeCSV).join(',') + '\n';
        rows.forEach(row => {
            csv += columns.map(c => escapeCSV(row[c])).join(',') + '\n';
        });

        // Download
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `qc_section_${resultIdx + 1}_${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a);
        a.click();
        URL.revokeObjectURL(url);
        a.remove();

        App.showToast(`Exported ${rows.length} rows to CSV`, 'success');
    },

    // Export grouped aggregation data to CSV
    exportGroupedAgg(resultIdx) {
        const rows = this._groupedAggData?.[resultIdx];
        if (!rows || rows.length === 0) {
            App.showToast('No grouped data to export', 'warning');
            return;
        }

        const columns = Object.keys(rows[0]);

        // Escape CSV values
        const escapeCSV = (val) => {
            if (val === null || val === undefined) return '';
            const str = String(val);
            if (str.includes(',') || str.includes('"') || str.includes('\n')) {
                return '"' + str.replace(/"/g, '""') + '"';
            }
            return str;
        };

        let csv = columns.map(escapeCSV).join(',') + '\n';
        rows.forEach(row => {
            csv += columns.map(c => escapeCSV(row[c])).join(',') + '\n';
        });

        // Download
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `grouped_aggregation_${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a);
        a.click();
        URL.revokeObjectURL(url);
        a.remove();

        App.showToast(`Exported ${rows.length} grouped rows to CSV`, 'success');
    }
};
