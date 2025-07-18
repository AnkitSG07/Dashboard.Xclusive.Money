// TradersPost Dashboard JavaScript

class TradersPostDashboard {
    constructor() {
        this.currentSection = 'dashboard';
        this.sidebarCollapsed = false;
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.setupMobileMenu();
        this.setupSPA();
        this.loadInitialData();
        this.updateBreadcrumb();
    }

    setupEventListeners() {
        // Sidebar navigation
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const section = link.dataset.section;
                
                if (section === 'logout') {
                    this.handleLogout();
                    return;
                }
                
                this.navigateToSection(section);
            });
        });

        // Mobile menu toggle
        const mobileMenuToggle = document.getElementById('mobileMenuToggle');
        if (mobileMenuToggle) {
            mobileMenuToggle.addEventListener('click', () => {
                this.toggleMobileMenu();
            });
        }

        // Sidebar toggle (close button)
        const sidebarToggle = document.getElementById('sidebarToggle');
        if (sidebarToggle) {
            sidebarToggle.addEventListener('click', () => {
                this.closeMobileMenu();
            });
        }

        // Connect broker buttons
        document.querySelectorAll('.connect-broker-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                this.showConnectBrokerDialog();
            });
        });

        // Watch video button
        document.querySelectorAll('.watch-video-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                this.showVideoDialog();
            });
        });

        // Create strategy/webhook buttons
        document.querySelectorAll('.btn').forEach(btn => {
            if (btn.textContent.includes('Create Strategy')) {
                btn.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.showCreateStrategyDialog();
                });
            } else if (btn.textContent.includes('Create Webhook')) {
                btn.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.showCreateWebhookDialog();
                });
            }
        });

        // Handle window resize
        window.addEventListener('resize', () => {
            this.handleResize();
        });

        // Handle clicks outside sidebar on mobile
        document.addEventListener('click', (e) => {
            if (window.innerWidth <= 991.98) {
                const sidebar = document.getElementById('sidebar');
                const mobileMenuToggle = document.getElementById('mobileMenuToggle');
                
                if (!sidebar.contains(e.target) && !mobileMenuToggle.contains(e.target)) {
                    this.closeMobileMenu();
                }
            }
        });
    }

    setupMobileMenu() {
        // Create overlay for mobile menu
        const overlay = document.createElement('div');
        overlay.className = 'sidebar-overlay';
        overlay.id = 'sidebarOverlay';
        document.body.appendChild(overlay);

        // Handle overlay click
        overlay.addEventListener('click', () => {
            this.closeMobileMenu();
        });
    }

    setupSPA() {
        // Handle browser back/forward buttons
        window.addEventListener('popstate', (e) => {
            if (e.state && e.state.section) {
                this.navigateToSection(e.state.section, false);
            }
        });

        // Set initial state
        const initialState = { section: this.currentSection };
        history.replaceState(initialState, '', `#${this.currentSection}`);
    }

    navigateToSection(section, pushState = true) {
        if (section === this.currentSection) return;

        // Hide current section
        const currentSectionEl = document.getElementById(`${this.currentSection}Section`);
        if (currentSectionEl) {
            currentSectionEl.classList.remove('active');
        }

        // Show new section
        const newSectionEl = document.getElementById(`${section}Section`);
        if (newSectionEl) {
            newSectionEl.classList.add('active');
        }

        // Toggle Connect Broker visibility
        const connectBtn = document.getElementById('add-account-btn');
        if (connectBtn) {
            connectBtn.style.display = section === 'dashboard' ? '' : 'none';
        }
        if (section !== 'dashboard') {
            const addSection = document.getElementById('add-account-section');
            if (addSection) addSection.style.display = 'none';
        }


        // Update navigation
        document.querySelectorAll('.nav-link').forEach(link => {
            link.classList.remove('active');
        });

        const activeLink = document.querySelector(`.nav-link[data-section="${section}"]`);
        if (activeLink) {
            activeLink.classList.add('active');
        }

        // Update current section
        this.currentSection = section;

        // Update browser history
        if (pushState) {
            const state = { section: section };
            history.pushState(state, '', `#${section}`);
        }

        // Update breadcrumb
        this.updateBreadcrumb();

        // Close mobile menu if open
        this.closeMobileMenu();

        // Load section-specific data
        this.loadSectionData(section);
    }

    updateBreadcrumb() {
        const breadcrumb = document.querySelector('.breadcrumb span');
        if (breadcrumb) {
            const sectionNames = {
                dashboard: 'Dashboard',
                notifications: 'Notifications',
                strategies: 'Strategies',
                subscriptions: 'Subscriptions',
                webhooks: 'Webhooks',
                brokers: 'Brokers',
                account: 'Account'
            };
            breadcrumb.textContent = sectionNames[this.currentSection] || 'Dashboard';
        }
    }

    toggleMobileMenu() {
        const sidebar = document.getElementById('sidebar');
        const overlay = document.getElementById('sidebarOverlay');
        
        if (sidebar.classList.contains('show')) {
            this.closeMobileMenu();
        } else {
            this.openMobileMenu();
        }
    }

    openMobileMenu() {
        const sidebar = document.getElementById('sidebar');
        const overlay = document.getElementById('sidebarOverlay');
        
        sidebar.classList.add('show');
        overlay.classList.add('show');
        document.body.style.overflow = 'hidden';
    }

    closeMobileMenu() {
        const sidebar = document.getElementById('sidebar');
        const overlay = document.getElementById('sidebarOverlay');
        
        sidebar.classList.remove('show');
        overlay.classList.remove('show');
        document.body.style.overflow = '';
    }

    handleResize() {
        if (window.innerWidth > 991.98) {
            this.closeMobileMenu();
        }
    }

    async loadInitialData() {
        try {
            // Load account information
            const accountResponse = await fetch('/api/account-info');
            const accountData = await accountResponse.json();
            this.updateAccountDisplay(accountData);

            // Load dashboard data
            const dashboardResponse = await fetch('/api/dashboard-data');
            const dashboardData = await dashboardResponse.json();
            this.updateDashboardDisplay(dashboardData);

        } catch (error) {
            console.error('Error loading initial data:', error);
            this.showNotification('Failed to load account data', 'error');
        }
    }

    async loadSectionData(section) {
        switch (section) {
            case 'dashboard':
                await this.loadDashboardData();
                break;
            case 'notifications':
                await this.loadNotifications();
                break;
            case 'strategies':
                await this.loadStrategies();
                break;
            case 'subscriptions':
                await this.loadSubscriptions();
                break;
            case 'webhooks':
                await this.loadWebhooks();
                break;
            case 'brokers':
                await this.loadBrokers();
                break;
            case 'account':
                await this.loadAccountData();
                break;
        }
    }

    async loadDashboardData() {
        // Dashboard data is already loaded in loadInitialData
    }

    async loadNotifications() {
        // Placeholder for notifications loading
        console.log('Loading notifications...');
    }

    async loadStrategies() {
        // Placeholder for strategies loading
        console.log('Loading strategies...');
    }

    async loadSubscriptions() {
        // Placeholder for subscriptions loading
        console.log('Loading subscriptions...');
    }

    async loadWebhooks() {
        // Placeholder for webhooks loading
        console.log('Loading webhooks...');
    }

    async loadBrokers() {
        // Placeholder for brokers loading
        console.log('Loading brokers...');
    }

    async loadAccountData() {
        // Placeholder for account data loading
        console.log('Loading account data...');
    }

    updateAccountDisplay(accountData) {
        const balanceElement = document.querySelector('.account-balance');
        if (balanceElement) {
            if (accountData.accounts && accountData.accounts.length > 0) {
                balanceElement.textContent = `$${accountData.balance.toFixed(2)}`;
            } else {
                balanceElement.textContent = '$0.00';
            }
        }

        const statusElement = document.querySelector('.status-text');
        if (statusElement) {
            statusElement.textContent = accountData.status || 'No account connected';
        }
    }

    updateDashboardDisplay(dashboardData) {
        // Update dashboard with real data when available
        console.log('Dashboard data loaded:', dashboardData);
    }

    showConnectBrokerDialog() {
        this.showNotification('Connect Broker feature would be implemented here', 'info');
    }

    showVideoDialog() {
        this.showNotification('Video tutorial would open here', 'info');
    }

    showCreateStrategyDialog() {
        this.showNotification('Create Strategy dialog would open here', 'info');
    }

    showCreateWebhookDialog() {
        this.showNotification('Create Webhook dialog would open here', 'info');
    }

    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `alert alert-${type === 'error' ? 'danger' : type === 'success' ? 'success' : 'info'} alert-dismissible fade show`;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 9999;
            min-width: 300px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        `;
        
        notification.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;

        document.body.appendChild(notification);

        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 5000);
    }

    handleLogout() {
        if (confirm('Are you sure you want to logout?')) {
            this.showNotification('Logout functionality would be implemented here', 'info');
        }
    }

    // Utility methods
    formatCurrency(amount) {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD'
        }).format(amount);
    }

    formatDate(date) {
        return new Intl.DateTimeFormat('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        }).format(new Date(date));
    }

    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
}

// Initialize the dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new TradersPostDashboard();
});

// Handle hash changes for direct navigation
window.addEventListener('hashchange', () => {
    const hash = window.location.hash.substring(1);
    if (hash && window.dashboard) {
        window.dashboard.navigateToSection(hash);
    }
});

// Check for hash on page load
document.addEventListener('DOMContentLoaded', () => {
    const hash = window.location.hash.substring(1);
    if (hash && window.dashboard) {
        setTimeout(() => {
            window.dashboard.navigateToSection(hash);
        }, 100);
    }
});
