/**
 * QC Tool - Charts Module
 * Handles all chart creation and visualizations using Chart.js and ECharts
 */

const QCCharts = {
    // Store chart instances to allow updates/destroy
    charts: {},

    // Chart color palette
    colors: {
        success: '#10b981',
        warning: '#f59e0b',
        error: '#ef4444',
        info: '#3b82f6',
        primary: '#6366f1',
        secondary: '#8b5cf6',
        gradient: {
            start: '#8b5cf6',
            end: '#ec4899'
        }
    },

    // Default chart configuration
    defaultConfig: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'bottom',
                labels: {
                    usePointStyle: true,
                    padding: 15,
                    font: { size: 12, weight: '500' }
                }
            },
            tooltip: {
                backgroundColor: 'rgba(0, 0, 0, 0.8)',
                padding: 12,
                titleFont: { size: 14, weight: 'bold' },
                bodyFont: { size: 13 },
                cornerRadius: 8,
                displayColors: true
            }
        },
        animation: {
            duration: 1000,
            easing: 'easeInOutQuart'
        }
    },

    /**
     * Initialize charts module
     */
    init() {
        console.log('QC Charts initialized');
    },

    /**
     * Destroy a chart by ID
     */
    destroyChart(chartId) {
        if (this.charts[chartId]) {
            this.charts[chartId].destroy();
            delete this.charts[chartId];
        }
    },

    /**
     * Destroy all charts
     */
    destroyAll() {
        Object.keys(this.charts).forEach(id => this.destroyChart(id));
    },

    /**
     * Create Pass/Fail Doughnut Chart
     */
    createPassFailDoughnut(canvasId, passCount, failCount) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        this.destroyChart(canvasId);

        const total = passCount + failCount;
        const passRate = total > 0 ? ((passCount / total) * 100).toFixed(1) : 0;

        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Passed', 'Failed'],
                datasets: [{
                    data: [passCount, failCount],
                    backgroundColor: [this.colors.success, this.colors.error],
                    borderWidth: 0,
                    hoverOffset: 10
                }]
            },
            options: {
                ...this.defaultConfig,
                cutout: '70%',
                plugins: {
                    ...this.defaultConfig.plugins,
                    legend: {
                        ...this.defaultConfig.plugins.legend,
                        position: 'bottom'
                    },
                    tooltip: {
                        ...this.defaultConfig.plugins.tooltip,
                        callbacks: {
                            label: function (context) {
                                const label = context.label || '';
                                const value = context.parsed || 0;
                                const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                                return `${label}: ${value} (${percentage}%)`;
                            }
                        }
                    },
                    // Center text plugin
                    beforeDraw: function (chart) {
                        const width = chart.width;
                        const height = chart.height;
                        const ctx = chart.ctx;

                        ctx.restore();
                        ctx.font = 'bold 24px Inter';
                        ctx.textBaseline = 'middle';
                        ctx.fillStyle = '#1f2937';

                        const text = `${passRate}%`;
                        const textX = Math.round((width - ctx.measureText(text).width) / 2);
                        const textY = height / 2 - 10;

                        ctx.fillText(text, textX, textY);

                        ctx.font = '12px Inter';
                        ctx.fillStyle = '#6b7280';
                        const subtext = 'Pass Rate';
                        const subtextX = Math.round((width - ctx.measureText(subtext).width) / 2);
                        const subtextY = height / 2 + 15;

                        ctx.fillText(subtext, subtextX, subtextY);
                        ctx.save();
                    }
                }
            },
            plugins: [{
                beforeDraw: function (chart) {
                    const width = chart.width;
                    const height = chart.height;
                    const ctx = chart.ctx;

                    ctx.restore();
                    ctx.font = 'bold 24px Inter';
                    ctx.textBaseline = 'middle';
                    ctx.fillStyle = '#1f2937';

                    const text = `${passRate}%`;
                    const textX = Math.round((width - ctx.measureText(text).width) / 2);
                    const textY = height / 2 - 10;

                    ctx.fillText(text, textX, textY);

                    ctx.font = '12px Inter';
                    ctx.fillStyle = '#6b7280';
                    const subtext = 'Pass Rate';
                    const subtextX = Math.round((width - ctx.measureText(subtext).width) / 2);
                    const subtextY = height / 2 + 15;

                    ctx.fillText(subtext, subtextX, subtextY);
                    ctx.save();
                }
            }]
        });

        return this.charts[canvasId];
    },

    /**
     * Create Horizontal Bar Chart (e.g., for rule execution times)
     */
    createHorizontalBarChart(canvasId, labels, data, title) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        this.destroyChart(canvasId);

        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Time (seconds)',
                    data: data,
                    backgroundColor: this.colors.primary,
                    borderRadius: 6,
                    barThickness: 30
                }]
            },
            options: {
                ...this.defaultConfig,
                indexAxis: 'y',
                plugins: {
                    ...this.defaultConfig.plugins,
                    title: {
                        display: !!title,
                        text: title,
                        font: { size: 16, weight: 'bold' },
                        padding: 20
                    },
                    legend: {
                        display: false
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        grid: {
                            display: true,
                            color: '#f3f4f6'
                        },
                        ticks: {
                            callback: function (value) {
                                return value.toFixed(2) + 's';
                            }
                        }
                    },
                    y: {
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });

        return this.charts[canvasId];
    },

    /**
     * Create Vertical Bar Chart
     */
    createBarChart(canvasId, labels, datasets, title) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        this.destroyChart(canvasId);

        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                ...this.defaultConfig,
                plugins: {
                    ...this.defaultConfig.plugins,
                    title: {
                        display: !!title,
                        text: title,
                        font: { size: 16, weight: 'bold' },
                        padding: 20
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: '#f3f4f6'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });

        return this.charts[canvasId];
    },

    /**
     * Create Line Chart
     */
    createLineChart(canvasId, labels, datasets, title) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        this.destroyChart(canvasId);

        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                ...this.defaultConfig,
                plugins: {
                    ...this.defaultConfig.plugins,
                    title: {
                        display: !!title,
                        text: title,
                        font: { size: 16, weight: 'bold' },
                        padding: 20
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: '#f3f4f6'
                        }
                    },
                    x: {
                        grid: {
                            display: false
                        }
                    }
                },
                elements: {
                    line: {
                        tension: 0.4
                    },
                    point: {
                        radius: 4,
                        hoverRadius: 6
                    }
                }
            }
        });

        return this.charts[canvasId];
    },

    /**
     * Export chart as PNG
     */
    exportChartPNG(chartId, filename) {
        const chart = this.charts[chartId];
        if (!chart) return;

        const url = chart.toBase64Image();
        const link = document.createElement('a');
        link.download = filename || `chart_${chartId}.png`;
        link.href = url;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    },

    /**
     * Create chart for Null Check - Null count per column
     */
    createNullCheckChart(canvasId, details) {
        if (!details || !details.violations) return null;

        const violations = details.violations;
        const labels = Object.keys(violations);
        const data = labels.map(col => violations[col].null_count || 0);

        return this.createHorizontalBarChart(
            canvasId,
            labels,
            data,
            'Null Values by Column'
        );
    },

    /**
     * Create chart for Pattern Check - Success rate per pattern
     */
    createPatternCheckChart(canvasId, statistics) {
        if (!statistics) return null;

        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        this.destroyChart(canvasId);

        const passRate = statistics.pass_rate || 0;
        const failRate = 100 - passRate;

        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Pattern Validation'],
                datasets: [{
                    label: 'Valid (%)',
                    data: [passRate],
                    backgroundColor: this.colors.success,
                    borderRadius: 6
                }, {
                    label: 'Invalid (%)',
                    data: [failRate],
                    backgroundColor: this.colors.error,
                    borderRadius: 6
                }]
            },
            options: {
                ...this.defaultConfig,
                indexAxis: 'y',
                plugins: {
                    ...this.defaultConfig.plugins,
                    title: {
                        display: true,
                        text: 'Pattern Match Success Rate',
                        font: { size: 16, weight: 'bold' }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        max: 100,
                        grid: {
                            color: '#f3f4f6'
                        },
                        ticks: {
                            callback: value => value + '%'
                        }
                    },
                    y: {
                        stacked: true,
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });

        return this.charts[canvasId];
    },

    /**
     * Create chart for Aggregation Check - Multi-series line chart
     */
    createAggregationLineChart(canvasId, groupedTable, groupByColumns) {
        const canvas = document.getElementById(canvasId);
        if (!canvas || !groupedTable || groupedTable.length === 0) return null;

        this.destroyChart(canvasId);

        // Extract labels (group values) and datasets (aggregated columns)
        const labels = groupedTable.map(row => {
            return groupByColumns.map(col => row[col]).join(', ');
        });

        // Find all aggregation columns (those starting with SUM, AVG, COUNT, etc.)
        const aggColumns = Object.keys(groupedTable[0]).filter(col =>
            !groupByColumns.includes(col) && /^(SUM|AVG|COUNT|MIN|MAX)\(/.test(col)
        );

        // Create dataset for each aggregation
        const datasets = aggColumns.map((col, idx) => {
            const colors = [this.colors.primary, this.colors.success, this.colors.warning, this.colors.error, this.colors.info];
            const color = colors[idx % colors.length];

            return {
                label: col,
                data: groupedTable.map(row => row[col]),
                borderColor: color,
                backgroundColor: color + '33',
                fill: false,
                tension: 0.4
            };
        });

        return this.createLineChart(canvasId, labels, datasets, 'Aggregation Trends');
    },

    /**
     * Create chart for Value Set Check - Invalid value frequency
     */
    createValueSetChart(canvasId, details) {
        if (!details || !details.invalid_value_frequency) return null;

        const freq = details.invalid_value_frequency.slice(0, 10); // Top 10
        const labels = freq.map(item => String(item.value).substring(0, 20));
        const data = freq.map(item => item.count);

        return this.createHorizontalBarChart(
            canvasId,
            labels,
            data,
            'Most Frequent Invalid Values'
        );
    },

    /**
     * Create chart for Uniqueness Check - Duplicate frequency
     */
    createUniquenessChart(canvasId, details) {
        if (!details || !details.duplicate_details) return null;

        const duplicates = details.duplicate_details.slice(0, 10); // Top 10
        const labels = duplicates.map(item => String(item.value).substring(0, 20));
        const data = duplicates.map(item => item.count);

        return this.createHorizontalBarChart(
            canvasId,
            labels,
            data,
            'Duplicate Value Frequency'
        );
    },

    /**
     * Get rule-specific chart for a result
     */
    createRuleChart(canvasId, result) {
        const ruleName = result.rule_name || '';
        const details = result.details || {};
        const statistics = result.statistics || {};

        // Dispatch to appropriate chart based on rule type
        if (ruleName.includes('Null Check')) {
            return this.createNullCheckChart(canvasId, details);
        } else if (ruleName.includes('Pattern Check')) {
            return this.createPatternCheckChart(canvasId, statistics);
        } else if (ruleName.includes('Aggregation Check') && details.grouped_table) {
            return this.createAggregationLineChart(canvasId, details.grouped_table, details.group_by || []);
        } else if (ruleName.includes('Value Set Check')) {
            return this.createValueSetChart(canvasId, details);
        } else if (ruleName.includes('Uniqueness Check')) {
            return this.createUniquenessChart(canvasId, details);
        }

        // Fallback for any other rule type
        return this.createGenericChart(canvasId, result);
    },

    /**
     * Create Quality Score Gauge using ECharts
     */
    createQualityGauge(containerId, passRate) {
        const container = document.getElementById(containerId);
        if (!container || typeof echarts === 'undefined') return null;

        // Destroy existing chart
        const existing = echarts.getInstanceByDom(container);
        if (existing) {
            existing.dispose();
        }

        const chart = echarts.init(container);

        // Determine color based on pass rate
        let color;
        if (passRate >= 90) {
            color = this.colors.success;
        } else if (passRate >= 70) {
            color = this.colors.warning;
        } else {
            color = this.colors.error;
        }

        const option = {
            series: [{
                type: 'gauge',
                startAngle: 200,
                endAngle: -20,
                min: 0,
                max: 100,
                splitNumber: 10,
                itemStyle: {
                    color: color
                },
                progress: {
                    show: true,
                    width: 18
                },
                pointer: {
                    show: false
                },
                axisLine: {
                    lineStyle: {
                        width: 18,
                        color: [[1, 'rgba(255,255,255,0.1)']]
                    }
                },
                axisTick: {
                    distance: -28,
                    splitNumber: 5,
                    lineStyle: {
                        width: 2,
                        color: '#fff'
                    }
                },
                splitLine: {
                    distance: -32,
                    length: 12,
                    lineStyle: {
                        width: 3,
                        color: '#fff'
                    }
                },
                axisLabel: {
                    distance: -50,
                    color: '#fff',
                    fontSize: 12
                },
                anchor: {
                    show: false
                },
                title: {
                    show: false
                },
                detail: {
                    valueAnimation: true,
                    width: '60%',
                    lineHeight: 40,
                    borderRadius: 8,
                    offsetCenter: [0, '0%'],
                    fontSize: 40,
                    fontWeight: 'bolder',
                    formatter: '{value}%',
                    color: 'inherit'
                },
                data: [{
                    value: passRate
                }]
            }]
        };

        chart.setOption(option);

        // Store in charts object for cleanup
        this.charts[containerId] = { dispose: () => chart.dispose() };

        return chart;
    },

    /**
     * Create chart for Comparison Results - Duplicates
     */
    createDuplicatesChart(canvasId, duplicateCount) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        this.destroyChart(canvasId);

        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Duplicate Records'],
                datasets: [{
                    label: 'Count',
                    data: [duplicateCount],
                    backgroundColor: this.colors.warning,
                    borderRadius: 6,
                    barThickness: 80
                }]
            },
            options: {
                ...this.defaultConfig,
                plugins: {
                    ...this.defaultConfig.plugins,
                    title: {
                        display: true,
                        text: 'Duplicate Records Found',
                        font: { size: 16, weight: 'bold' }
                    },
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: '#f3f4f6'
                        }
                    }
                }
            }
        });

        return this.charts[canvasId];
    },

    /**
     * Create chart for Comparison Results - Source Distribution
     */
    createSourceComparisonChart(canvasId, uniqueData, duplicateCount) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        this.destroyChart(canvasId);

        // Extract unique counts per source
        const labels = [];
        const data = [];
        const colors = [this.colors.info, this.colors.success, this.colors.primary];

        if (uniqueData) {
            Object.entries(uniqueData).forEach(([sourceName, info], idx) => {
                labels.push(`Unique to ${sourceName}`);
                data.push(info.count || 0);
            });
        }

        if (duplicateCount > 0) {
            labels.push('In Multiple Sources');
            data.push(duplicateCount);
        }

        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: labels,
                datasets: [{
                    data: data,
                    backgroundColor: [...colors.slice(0, labels.length)],
                    borderWidth: 2,
                    borderColor: '#1a1f2e'
                }]
            },
            options: {
                ...this.defaultConfig,
                plugins: {
                    ...this.defaultConfig.plugins,
                    title: {
                        display: true,
                        text: 'Record Distribution Across Sources',
                        font: { size: 16, weight: 'bold' }
                    }
                }
            }
        });

        return this.charts[canvasId];
    },

    /**
     * Create chart for comparison result based on type
     */
    createComparisonChart(canvasId, result) {
        const ruleName = result.rule_name || '';

        // Handle duplicates
        if (ruleName.includes('Duplicates')) {
            const duplicateCount = result.statistics?.duplicate_count ||
                result.details?.count ||
                (result.failed_rows ? result.failed_rows.length : 0);
            return this.createDuplicatesChart(canvasId, duplicateCount);
        }

        // Handle source comparison overview
        if (result.details && result.details.unique) {
            const duplicateCount = result.details.duplicates?.count || 0;
            return this.createSourceComparisonChart(canvasId, result.details.unique, duplicateCount);
        }

        return null;
    },

    /**
     * Create Generic Chart for any rule (Fallback)
     */
    createGenericChart(canvasId, result) {
        const stats = result.statistics || {};

        // 1. Try to find interesting numeric statistics
        const numericStats = Object.entries(stats)
            .filter(([k, v]) => typeof v === 'number' && k !== 'total_rows' && k !== 'processed_rows' && !k.includes('id'))
            .map(([k, v]) => ({ label: k.replace(/_/g, ' '), value: v }));

        if (numericStats.length > 0) {
            return this.createBarChart(
                canvasId,
                numericStats.map(s => s.label),
                [{
                    label: 'Count',
                    data: numericStats.map(s => s.value),
                    backgroundColor: this.colors.primary,
                    borderRadius: 6
                }],
                'Rule Statistics'
            );
        }

        // 2. Fallback to Simple Pass/Fail pie if pass/fail counts exist
        if (stats.passed_rows !== undefined && stats.failed_rows !== undefined) {
            return this.createPassFailDoughnut(canvasId, stats.passed_rows, stats.failed_rows);
        }

        // 3. Fallback to just "Failed Rows" count if nothing else
        const failedCount = result.failed_row_count || (result.failed_rows ? result.failed_rows.length : 0);
        if (failedCount > 0) {
            return this.createHorizontalBarChart(
                canvasId,
                ['Failed Rows'],
                [failedCount],
                'Rule Violations'
            );
        }

        return null;
    }
};

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    QCCharts.init();
});
