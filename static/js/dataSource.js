/**
 * QC Tool - Data Source Module
 * Handles file uploads and database queries
 */

const DataSource = {
    selectedFiles: [],
    currentSource: 'file',

    init() {
        this.setupSourceSelector();
        this.setupFileUpload();
        this.setupPostgres();
        this.setupAthena();
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

    // File Upload
    setupFileUpload() {
        const uploadZone = document.getElementById('uploadZone');
        const fileInput = document.getElementById('fileInput');
        const loadBtn = document.getElementById('loadFilesBtn');

        // Click to browse
        uploadZone.addEventListener('click', () => fileInput.click());

        // File selection
        fileInput.addEventListener('change', (e) => {
            this.addFiles(Array.from(e.target.files));
        });

        // Drag and drop
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

        // Load button
        loadBtn.addEventListener('click', () => this.uploadFiles());
    },

    addFiles(files) {
        const validExtensions = ['.csv', '.xlsx', '.xls'];

        files.forEach(file => {
            const ext = '.' + file.name.split('.').pop().toLowerCase();
            if (validExtensions.includes(ext)) {
                // Check for duplicates
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

            App.addSession({
                session_id: data.session_id,
                source: 'file',
                row_count: data.row_count,
                columns: data.columns
            });

            this.showDataPreview(data);
            this.selectedFiles = [];
            this.renderFileList();

            App.showToast(`Loaded ${data.row_count} rows from ${data.files.length} file(s)`, 'success');

        } catch (error) {
            App.showToast(`Upload failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    },

    // PostgreSQL
    setupPostgres() {
        document.getElementById('testPgBtn').addEventListener('click', () => {
            this.testConnection('postgres');
        });

        document.getElementById('runPgQueryBtn').addEventListener('click', () => {
            this.runQuery('postgres');
        });
    },

    // Athena
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
                session_id: data.session_id,
                source: source,
                row_count: data.row_count,
                columns: data.columns
            });

            this.showDataPreview(data);
            App.showToast(`Query returned ${data.row_count} rows`, 'success');

        } catch (error) {
            App.showToast(`Query failed: ${error.message}`, 'error');
        } finally {
            App.hideLoading();
        }
    },

    showDataPreview(data) {
        const preview = document.getElementById('dataPreview');
        const thead = document.getElementById('previewTableHead');
        const tbody = document.getElementById('previewTableBody');

        document.getElementById('previewRowCount').textContent = `${data.row_count} rows`;
        document.getElementById('previewColCount').textContent = `${data.columns.length} columns`;

        // Headers
        thead.innerHTML = `<tr>${data.columns.map(col =>
            `<th>${col}<br><span style="font-weight:normal;font-size:0.75rem;color:var(--text-muted)">${data.dtypes[col]}</span></th>`
        ).join('')}</tr>`;

        // Body (first 100 rows)
        tbody.innerHTML = data.preview.map(row =>
            `<tr>${data.columns.map(col =>
                `<td>${row[col] !== null && row[col] !== undefined ? row[col] : '<span style="color:var(--text-muted)">null</span>'}</td>`
            ).join('')}</tr>`
        ).join('');

        preview.classList.remove('hidden');
    }
};
