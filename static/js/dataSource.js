/**
 * QC Tool - Data Source Module
 * Handles file uploads, database queries, source management, and data viewing
 */

const DataSource = {
    selectedFiles: [],
    currentSource: 'file',

    // Data Viewer state
    viewerSession: null,
    viewerPage: 0,
    viewerPageSize: 50,
    viewerTotalRows: 0,
    viewerColumns: [],

    // Rename state
    renameSessionId: null,

    init() {
        this.setupSourceSelector();
        this.setupFileUpload();
        this.setupPostgres();
        this.setupAthena();
        this.setupDataViewer();
        this.setupRefreshButton();
        this.setupCredentialsManager();
        this.setupRenameModal();
        this.loadSessions();
    },

    // Source selector
    setupSourceSelector() {
        document.querySelectorAll('.source-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const source = btn.dataset.source;
                this.switchSource(source);
            });
        });
    },

    switchSource(source) {
        this.currentSource = source;

        // Update buttons
        document.querySelectorAll('.source-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.source === source);
        });

        // Update panels
        document.querySelectorAll('.source-panel').forEach(panel => {
            panel.classList.remove('active');
        });

        const panelMap = {
            'file': 'filePanel',
            'postgres': 'postgresPanel',
            'athena': 'athenaPanel'
        };

        document.getElementById(panelMap[source]).classList.add('active');
    },

    // Refresh button
    setupRefreshButton() {
        const btn = document.getElementById('refreshSourcesBtn');
        if (btn) {
            btn.addEventListener('click', () => this.loadSessions());
        }
    },

    // Load all sessions from backend
    async loadSessions() {
        try {
            const response = await fetch(`${App.API_BASE}/api/data/sources`);
            const data = await response.json();

            if (data.success) {
                App.state.sessions = data.sources;
                this.renderSourcesList(data.sources);
                App.updateQCView();
                App.updateCompareView();
            }
        } catch (error) {
            console.error('Failed to load sessions:', error);
        }
    },

    // Render sources list
    renderSourcesList(sessions) {
        const panel = document.getElementById('loadedSourcesPanel');
        const container = document.getElementById('sourcesList');
        const countSpan = document.getElementById('sourceCount');

        if (sessions.length === 0) {
            panel.classList.add('hidden');
            return;
        }

        panel.classList.remove('hidden');
        countSpan.textContent = `(${sessions.length})`;

        container.innerHTML = sessions.map(session => {
            const iconClass = session.source === 'file' ? 'file' :
                session.source === 'postgres' ? 'postgres' : 'athena';
            const iconSvg = this.getSourceIcon(session.source);

            // Show SQL button only for database sources with a query
            const showSqlBtn = session.query ? `
                <button class="btn btn-outline btn-sm" onclick="DataSource.showSqlQuery('${session.source_id}')" title="View SQL Query">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                        <polyline points="16 18 22 12 16 6"></polyline>
                        <polyline points="8 6 2 12 8 18"></polyline>
                    </svg>
                    SQL
                </button>
            ` : '';

            return `
                <div class="source-card" data-session-id="${session.source_id}">
                    <div class="source-card-header">
                        <div class="source-card-icon ${iconClass}">
                            ${iconSvg}
                        </div>
                        <div class="source-card-info">
                            <div class="source-card-name">${session.source_name}</div>
                            <div class="source-card-meta">
                                <span>${session.row_count.toLocaleString()} rows</span>
                                <span>${session.column_count} columns</span>
                            </div>
                        </div>
                    </div>
                    <div class="source-card-actions">
                        ${showSqlBtn}
                        <button class="btn btn-outline btn-sm" onclick="DataSource.viewData('${session.source_id}', '${session.source_name}', ${session.row_count})">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                                <circle cx="12" cy="12" r="3"></circle>
                            </svg>
                            View
                        </button>
                        <button class="btn btn-success btn-sm" onclick="DataSource.selectForQC('${session.source_id}')">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                <path d="M9 12l2 2 4-4"></path>
                                <circle cx="12" cy="12" r="10"></circle>
                            </svg>
                            QC
                        </button>
                        <button class="btn btn-outline btn-sm" onclick="DataSource.showRenameModal('${session.source_id}', '${session.source_name.replace(/'/g, "\\'")}')">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                                <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
                            </svg>
                        </button>
                        <button class="btn btn-danger btn-sm" onclick="DataSource.deleteSession('${session.source_id}')">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                                <polyline points="3 6 5 6 21 6"></polyline>
                                <path d="M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2"></path>
                            </svg>
                        </button>
                    </div>
                </div>
            `;
        }).join('');
    },

    getSourceIcon(source) {
        if (source === 'file') {
            return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M13 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V9z"></path>
                <polyline points="13 2 13 9 20 9"></polyline>
            </svg>`;
        } else if (source === 'postgres') {
            return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
                <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
                <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
            </svg>`;
        } else {
            return `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"></path>
            </svg>`;
        }
    },

    // Select for QC
    selectForQC(sessionId) {
        App.state.activeSession = sessionId;
        App.switchView('qc');
        App.showToast('Source selected for QC checks', 'success');
    },

    // Delete session
    async deleteSession(sourceId) {
        const warning = '⚠️ WARNING: Deleting this source will permanently remove:\n\n' +
            '• The source metadata\n' +
            '• All associated data rows\n\n' +
            'This action cannot be undone.\n\n' +
            'Are you sure you want to delete this data source?';

        if (!confirm(warning)) return;

        try {
            const response = await fetch(`${App.API_BASE}/api/data/sources/${sourceId}`, {
                method: 'DELETE'
            });

            if (response.ok) {
                await this.loadSessions();
                App.showToast('Data source deleted', 'success');
            } else {
                throw new Error('Delete failed');
            }
        } catch (error) {
            App.showToast(`Failed to delete: ${error.message}`, 'error');
        }
    },

    // ========================================
    // Data Viewer
    // ========================================

    setupDataViewer() {
        // Page size change
        document.getElementById('pageSizeSelect').addEventListener('change', (e) => {
            this.viewerPageSize = parseInt(e.target.value);
            this.viewerPage = 0;
            this.loadPageData();
        });

        // Pagination
        document.getElementById('prevPageBtn').addEventListener('click', () => {
            if (this.viewerPage > 0) {
                this.viewerPage--;
                this.loadPageData();
            }
        });

        document.getElementById('nextPageBtn').addEventListener('click', () => {
            const maxPage = Math.ceil(this.viewerTotalRows / this.viewerPageSize) - 1;
            if (this.viewerPage < maxPage) {
                this.viewerPage++;
                this.loadPageData();
            }
        });

        // Modal close
        document.getElementById('dataViewerModal').addEventListener('click', (e) => {
            if (e.target.closest('.modal-close') || e.target === e.currentTarget) {
                App.hideModal('dataViewerModal');
            }
        });
    },

    async viewData(sessionId, sourceName, totalRows) {
        this.viewerSession = sessionId;
        this.viewerTotalRows = totalRows;
        this.viewerPage = 0;

        document.getElementById('dataViewerTitle').textContent = sourceName;
        document.getElementById('dataViewerInfo').textContent = `${totalRows.toLocaleString()} rows`;

        App.showModal('dataViewerModal');
        await this.loadPageData();
    },

    async loadPageData() {
        const offset = this.viewerPage * this.viewerPageSize;

        try {
            const response = await fetch(
                `${App.API_BASE}/api/data/preview/${this.viewerSession}?offset=${offset}&limit=${this.viewerPageSize}`
            );
            const data = await response.json();

            if (!data.success) throw new Error(data.error);

            this.viewerColumns = data.columns;
            this.renderDataTable(data);
            this.updatePagination();

        } catch (error) {
            App.showToast(`Failed to load data: ${error.message}`, 'error');
        }
    },

    renderDataTable(data) {
        const thead = document.getElementById('dataViewerHead');
        const tbody = document.getElementById('dataViewerBody');

        // Headers with data types
        thead.innerHTML = `<tr>${data.columns.map(col =>
            `<th>${col}<br><span style="font-weight:normal;font-size:0.75rem;color:var(--text-muted)">${data.dtypes[col]}</span></th>`
        ).join('')}</tr>`;

        // Data rows
        tbody.innerHTML = data.data.map(row =>
            `<tr>${data.columns.map(col => {
                const val = row[col];
                if (val === null || val === undefined) {
                    return `<td><span style="color:var(--text-muted);font-style:italic">null</span></td>`;
                }
                return `<td>${val}</td>`;
            }).join('')}</tr>`
        ).join('');
    },

    updatePagination() {
        const totalPages = Math.ceil(this.viewerTotalRows / this.viewerPageSize);
        const start = this.viewerPage * this.viewerPageSize + 1;
        const end = Math.min(start + this.viewerPageSize - 1, this.viewerTotalRows);

        document.getElementById('paginationInfo').textContent =
            `Showing ${start.toLocaleString()}-${end.toLocaleString()} of ${this.viewerTotalRows.toLocaleString()}`;
        document.getElementById('pageIndicator').textContent =
            `Page ${this.viewerPage + 1} of ${totalPages}`;

        document.getElementById('prevPageBtn').disabled = this.viewerPage === 0;
        document.getElementById('nextPageBtn').disabled = this.viewerPage >= totalPages - 1;
    },

    // ========================================
    // File Upload
    // ========================================

    setupFileUpload() {
        const uploadZone = document.getElementById('uploadZone');
        const fileInput = document.getElementById('fileInput');
        const loadBtn = document.getElementById('loadFilesBtn');

        uploadZone.addEventListener('click', () => fileInput.click());

        fileInput.addEventListener('change', (e) => {
            this.addFiles(Array.from(e.target.files));
        });

        uploadZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadZone.classList.add('dragover');
        });

        uploadZone.addEventListener('dragleave', () => {
            uploadZone.classList.remove('dragover');
        });

        uploadZone.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadZone.classList.remove('dragover');
            this.addFiles(Array.from(e.dataTransfer.files));
        });

        loadBtn.addEventListener('click', () => this.uploadFiles());
    },

    addFiles(files) {
        const validExtensions = ['.csv', '.xlsx', '.xls'];

        files.forEach(file => {
            const ext = '.' + file.name.split('.').pop().toLowerCase();
            if (validExtensions.includes(ext)) {
                if (!this.selectedFiles.find(f => f.name === file.name)) {
                    this.selectedFiles.push(file);
                }
            } else {
                App.showToast(`Invalid file type: ${file.name}`, 'error');
            }
        });

        this.renderFileList();
    },

    removeFile(index) {
        this.selectedFiles.splice(index, 1);
        this.renderFileList();
    },

    renderFileList() {
        const container = document.getElementById('fileList');
        const loadBtn = document.getElementById('loadFilesBtn');

        if (this.selectedFiles.length === 0) {
            container.innerHTML = '';
            loadBtn.disabled = true;
            return;
        }

        container.innerHTML = this.selectedFiles.map((file, index) => `
            <div class="file-item">
                <div class="file-item-info">
                    <div class="file-item-icon">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M13 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V9z"></path>
                            <polyline points="13 2 13 9 20 9"></polyline>
                        </svg>
                    </div>
                    <div>
                        <div class="file-item-name">${file.name}</div>
                        <div class="file-item-size">${this.formatFileSize(file.size)}</div>
                    </div>
                </div>
                <button class="file-item-remove" onclick="DataSource.removeFile(${index})">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18">
                        <line x1="18" y1="6" x2="6" y2="18"></line>
                        <line x1="6" y1="6" x2="18" y2="18"></line>
                    </svg>
                </button>
            </div>
        `).join('');

        loadBtn.disabled = false;
    },

    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },

    async uploadFiles() {
        if (this.selectedFiles.length === 0) return;

        App.showLoading('Uploading files...');

        const formData = new FormData();
        this.selectedFiles.forEach(file => {
            formData.append('files', file);
        });

        try {
            const response = await fetch(`${App.API_BASE}/api/data/upload`, {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Upload failed');
            }

            // Handle new multi-session response
            const successful = data.sources.filter(s => s.success);
            const failed = data.sources.filter(s => !s.success);

            if (successful.length > 0) {
                // Add sessions to app state
                successful.forEach(s => {
                    App.addSession({
                        source_id: s.source_id,
                        source: 'file',
                        source_name: s.filename,
                        row_count: s.row_count,
                        column_count: s.columns.length,
                        columns: s.columns
                    });
                });

                await this.loadSessions();
                this.selectedFiles = [];
                this.renderFileList();

                App.showToast(
                    `Loaded ${successful.length} file(s) successfully` +
                    (failed.length > 0 ? `, ${failed.length} failed` : ''),
                    failed.length > 0 ? 'warning' : 'success'
                );
            } else {
                throw new Error('All files failed to load');
            }

        } catch (error) {
            App.showToast(`Upload failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    },

    // ========================================
    // Database Queries
    // ========================================

    setupPostgres() {
        document.getElementById('testPgBtn').addEventListener('click', () => {
            this.testConnection('postgres');
        });

        document.getElementById('runPgQueryBtn').addEventListener('click', () => {
            this.runQuery('postgres');
        });
    },

    setupAthena() {
        document.getElementById('testAthenaBtn').addEventListener('click', () => {
            this.testConnection('athena');
        });

        document.getElementById('runAthenaQueryBtn').addEventListener('click', () => {
            this.runQuery('athena');
        });
    },

    async testConnection(source) {
        App.showLoading('Testing connection...');

        try {
            let payload;

            if (source === 'postgres') {
                payload = {
                    source: 'postgres',
                    host: document.getElementById('pgHost').value,
                    port: document.getElementById('pgPort').value,
                    database: document.getElementById('pgDatabase').value,
                    user: document.getElementById('pgUser').value,
                    password: document.getElementById('pgPassword').value
                };
            } else {
                payload = {
                    source: 'athena',
                    region: document.getElementById('athenaRegion').value,
                    database: document.getElementById('athenaDatabase').value,
                    s3_output: document.getElementById('athenaS3Output').value,
                    access_key: document.getElementById('athenaAccessKey').value,
                    secret_key: document.getElementById('athenaSecretKey').value,
                    workgroup: document.getElementById('athenaWorkgroup').value
                };
            }

            const response = await fetch(`${App.API_BASE}/api/data/test-connection`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            const data = await response.json();

            if (data.success) {
                App.showToast('Connection successful!', 'success');
            } else {
                App.showToast(`Connection failed: ${data.message}`, 'error');
            }

        } catch (error) {
            App.showToast(`Connection test failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    },

    async runQuery(source) {
        let query, payload;

        if (source === 'postgres') {
            query = document.getElementById('pgQuery').value.trim();
            payload = {
                source: 'postgres',
                query: query,
                host: document.getElementById('pgHost').value,
                port: document.getElementById('pgPort').value,
                database: document.getElementById('pgDatabase').value,
                user: document.getElementById('pgUser').value,
                password: document.getElementById('pgPassword').value
            };
        } else {
            query = document.getElementById('athenaQuery').value.trim();
            payload = {
                source: 'athena',
                query: query,
                region: document.getElementById('athenaRegion').value,
                database: document.getElementById('athenaDatabase').value,
                s3_output: document.getElementById('athenaS3Output').value,
                access_key: document.getElementById('athenaAccessKey').value,
                secret_key: document.getElementById('athenaSecretKey').value,
                workgroup: document.getElementById('athenaWorkgroup').value
            };
        }

        if (!query) {
            App.showToast('Please enter a query', 'warning');
            return;
        }

        App.showLoading('Executing query...');

        try {
            const response = await fetch(`${App.API_BASE}/api/data/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Query failed');
            }

            App.addSession({
                source_id: data.source_id,
                source: source,
                source_name: `${source.toUpperCase()} Query`,
                row_count: data.row_count,
                column_count: data.columns.length,
                columns: data.columns
            });

            await this.loadSessions();
            App.showToast(`Query returned ${data.row_count} rows`, 'success');

        } catch (error) {
            App.showToast(`Query failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    },

    // ========================================
    // Credentials Management
    // ========================================

    setupCredentialsManager() {
        // PostgreSQL credentials
        document.getElementById('savePgCredentialsBtn')?.addEventListener('click', () => {
            this.saveCredentials('postgres');
        });
        document.getElementById('pgSavedCredentials')?.addEventListener('change', (e) => {
            if (e.target.value) this.loadCredentials('postgres', e.target.value);
        });
        document.getElementById('deletePgCredentialsBtn')?.addEventListener('click', () => {
            const name = document.getElementById('pgSavedCredentials').value;
            if (name) this.deleteCredentials('postgres', name);
        });

        // Athena credentials
        document.getElementById('saveAthenaCredentialsBtn')?.addEventListener('click', () => {
            this.saveCredentials('athena');
        });
        document.getElementById('athenaSavedCredentials')?.addEventListener('change', (e) => {
            if (e.target.value) this.loadCredentials('athena', e.target.value);
        });
        document.getElementById('deleteAthenaCredentialsBtn')?.addEventListener('click', () => {
            const name = document.getElementById('athenaSavedCredentials').value;
            if (name) this.deleteCredentials('athena', name);
        });

        // Load saved credentials lists
        this.refreshCredentialsList('postgres');
        this.refreshCredentialsList('athena');
    },

    async saveCredentials(source) {
        let name, credentials;

        if (source === 'postgres') {
            name = document.getElementById('pgCredentialName').value.trim();
            if (!name) {
                App.showToast('Enter a name for these credentials', 'warning');
                return;
            }
            credentials = {
                host: document.getElementById('pgHost').value,
                port: document.getElementById('pgPort').value,
                database: document.getElementById('pgDatabase').value,
                user: document.getElementById('pgUser').value,
                password: document.getElementById('pgPassword').value
            };
        } else {
            name = document.getElementById('athenaCredentialName').value.trim();
            if (!name) {
                App.showToast('Enter a name for these credentials', 'warning');
                return;
            }
            credentials = {
                region: document.getElementById('athenaRegion').value,
                workgroup: document.getElementById('athenaWorkgroup').value,
                database: document.getElementById('athenaDatabase').value,
                s3_output: document.getElementById('athenaS3Output').value,
                access_key: document.getElementById('athenaAccessKey').value,
                secret_key: document.getElementById('athenaSecretKey').value
            };
        }

        // Save to SQLite via API
        try {
            const response = await fetch(`${App.API_BASE}/api/storage/credentials/${source}/${encodeURIComponent(name)}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(credentials)
            });
            const data = await response.json();

            if (data.success) {
                // Clear name input
                if (source === 'postgres') {
                    document.getElementById('pgCredentialName').value = '';
                } else {
                    document.getElementById('athenaCredentialName').value = '';
                }
                this.refreshCredentialsList(source);
                App.showToast(`Credentials "${name}" saved`, 'success');
            } else {
                App.showToast(`Failed to save credentials: ${data.error}`, 'error');
            }
        } catch (error) {
            App.showToast(`Failed to save credentials: ${error.message}`, 'error');
        }
    },

    async loadCredentials(source, name) {
        // Load from SQLite via API
        try {
            const response = await fetch(`${App.API_BASE}/api/storage/credentials/${source}/${encodeURIComponent(name)}`);
            const data = await response.json();

            if (!data.success || !data.data) {
                App.showToast('Credentials not found', 'warning');
                return;
            }

            const credentials = data.data;

            if (source === 'postgres') {
                document.getElementById('pgHost').value = credentials.host || '';
                document.getElementById('pgPort').value = credentials.port || '5432';
                document.getElementById('pgDatabase').value = credentials.database || '';
                document.getElementById('pgUser').value = credentials.user || '';
                document.getElementById('pgPassword').value = credentials.password || '';
            } else {
                document.getElementById('athenaRegion').value = credentials.region || 'us-east-1';
                document.getElementById('athenaWorkgroup').value = credentials.workgroup || 'primary';
                document.getElementById('athenaDatabase').value = credentials.database || '';
                document.getElementById('athenaS3Output').value = credentials.s3_output || '';
                document.getElementById('athenaAccessKey').value = credentials.access_key || '';
                document.getElementById('athenaSecretKey').value = credentials.secret_key || '';
            }

            App.showToast(`Credentials "${name}" loaded`, 'success');
        } catch (error) {
            App.showToast(`Failed to load credentials: ${error.message}`, 'error');
        }
    },

    async deleteCredentials(source, name) {
        if (!confirm(`Delete saved credentials "${name}"?`)) return;

        try {
            const response = await fetch(`${App.API_BASE}/api/storage/credentials/${source}/${encodeURIComponent(name)}`, {
                method: 'DELETE'
            });
            const data = await response.json();

            if (data.success) {
                this.refreshCredentialsList(source);
                App.showToast(`Credentials "${name}" deleted`, 'success');
            } else {
                App.showToast(`Failed to delete credentials: ${data.error}`, 'error');
            }
        } catch (error) {
            App.showToast(`Failed to delete credentials: ${error.message}`, 'error');
        }
    },

    async refreshCredentialsList(source) {
        try {
            const response = await fetch(`${App.API_BASE}/api/storage/credentials/${source}`);
            const data = await response.json();

            const names = data.success ? data.names : [];

            let select;
            if (source === 'postgres') {
                select = document.getElementById('pgSavedCredentials');
            } else {
                select = document.getElementById('athenaSavedCredentials');
            }

            if (!select) return;

            select.innerHTML = '<option value="">Select saved...</option>' +
                names.map(n => `<option value="${n}">${n}</option>`).join('');
        } catch (error) {
            console.error('Failed to refresh credentials list:', error);
        }
    },

    // ========================================
    // Rename Source
    // ========================================

    setupRenameModal() {
        document.getElementById('confirmRenameBtn')?.addEventListener('click', () => {
            this.renameSource();
        });

        // Handle Enter key in rename input
        document.getElementById('newSourceName')?.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.renameSource();
            }
        });
    },

    showRenameModal(sessionId, currentName) {
        this.renameSessionId = sessionId;
        document.getElementById('newSourceName').value = currentName;
        App.showModal('renameSourceModal');
        // Focus the input
        setTimeout(() => document.getElementById('newSourceName').focus(), 100);
    },

    async renameSource() {
        const newName = document.getElementById('newSourceName').value.trim();

        if (!newName) {
            App.showToast('Please enter a name', 'warning');
            return;
        }

        if (!this.renameSessionId) return;

        try {
            const response = await fetch(`${App.API_BASE}/api/data/sources/${this.renameSessionId}/rename`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: newName })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error || 'Rename failed');
            }

            App.hideModal('renameSourceModal');
            await this.loadSessions();
            App.showToast(`Source renamed to "${newName}"`, 'success');

        } catch (error) {
            App.showToast(`Rename failed: ${error.message}`, 'error');
        }
    },

    // ========================================
    // View SQL Query
    // ========================================

    showSqlQuery(sessionId) {
        const session = App.state.sessions.find(s => s.source_id === sessionId);
        if (!session || !session.query) {
            App.showToast('No SQL query available for this source', 'warning');
            return;
        }

        // Set modal content
        document.getElementById('sqlQuerySourceName').textContent = session.source_name;
        document.getElementById('sqlQueryContent').textContent = session.query;

        App.showModal('sqlQueryModal');
    },

    copySqlQuery() {
        const queryText = document.getElementById('sqlQueryContent').textContent;
        navigator.clipboard.writeText(queryText).then(() => {
            App.showToast('SQL query copied to clipboard', 'success');
        }).catch(() => {
            App.showToast('Failed to copy to clipboard', 'error');
        });
    }
};
