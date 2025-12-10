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

        container.innerHTML = results.map(result => {
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

            // Render failed rows preview if available
            let failedRowsHtml = '';
            if (result.failed_rows && result.failed_rows.length > 0) {
                const columns = Object.keys(result.failed_rows[0]);
                const displayCols = columns.slice(0, 5); // Limit columns
                const displayRows = result.failed_rows.slice(0, 10); // Limit rows

                failedRowsHtml = `
                    <div style="margin-top: 1rem;">
                        <h4 style="font-size: 0.9rem; margin-bottom: 0.5rem;">
                            Failed Rows Preview 
                            <span style="font-weight: normal; color: var(--text-muted)">
                                (${result.failed_row_count || result.failed_rows.length} total)
                            </span>
                        </h4>
                        <div class="table-container" style="max-height: 200px;">
                            <table class="data-table">
                                <thead>
                                    <tr>${displayCols.map(c => `<th>${c}</th>`).join('')}${columns.length > 5 ? '<th>...</th>' : ''}</tr>
                                </thead>
                                <tbody>
                                    ${displayRows.map(row =>
                    `<tr>${displayCols.map(c => `<td>${row[c] !== null ? row[c] : '<span style="color:var(--text-muted)">null</span>'}</td>`).join('')}${columns.length > 5 ? '<td>...</td>' : ''}</tr>`
                ).join('')}
                                </tbody>
                            </table>
                        </div>
                        ${result.failed_rows.length > 10 ? `<p style="color:var(--text-muted);font-size:0.85rem;margin-top:0.5rem">Showing 10 of ${result.failed_row_count || result.failed_rows.length} rows. Export for full data.</p>` : ''}
                    </div>
                `;
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

            return `
                <div class="result-card ${passed ? 'passed' : 'failed'}">
                    <div class="result-header">
                        <span class="result-title">${result.rule_name || 'Check'}</span>
                        <span class="result-badge ${passed ? 'pass' : 'fail'}">${passed ? 'PASS' : 'FAIL'}</span>
                    </div>
                    <p class="result-message">${result.message || ''}</p>
                    ${statsHtml ? `<div class="result-details">${statsHtml}</div>` : ''}
                    ${violationsHtml}
                    ${failedRowsHtml}
                </div>
            `;
        }).join('');
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
    }
};
