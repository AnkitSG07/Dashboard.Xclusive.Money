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
function initializeNotificationsPanel() {
    const toggle = document.getElementById('notificationsToggle');
    const panel = document.getElementById('notificationsPanel');
    const backdrop = document.getElementById('notificationsBackdrop');
    const list = document.getElementById('notificationsList');
    const loadingState = document.getElementById('notificationsLoading');
    const emptyState = document.getElementById('notificationsEmpty');
    const closeButton = document.getElementById('notificationsClose');
    const closeFooterButton = document.getElementById('notificationsCloseFooter');
    const clearButton = document.getElementById('notificationsClear');

    if (!toggle || !panel || !backdrop || !list || !loadingState || !emptyState) {
        return;
    }

    toggle.setAttribute('aria-expanded', 'false');

    const emptyMessageEl = emptyState.querySelector('span');
    const emptyIconEl = emptyState.querySelector('i');
    const defaultEmptyMessage = emptyMessageEl ? emptyMessageEl.textContent : 'No notifications yet';
    const defaultEmptyIcon = emptyIconEl ? emptyIconEl.className : 'bi bi-inbox';

    const iconMap = {
        ERROR: 'bi-exclamation-octagon-fill',
        WARNING: 'bi-exclamation-triangle-fill',
        SUCCESS: 'bi-check-circle-fill',
        INFO: 'bi-info-circle-fill'
    };

    function setPanelOpen(isOpen) {
        panel.classList.toggle('is-active', isOpen);
        backdrop.classList.toggle('is-active', isOpen);
        panel.setAttribute('aria-hidden', (!isOpen).toString());
        backdrop.setAttribute('aria-hidden', (!isOpen).toString());
        toggle.setAttribute('aria-expanded', isOpen.toString());

        if (isOpen) {
            document.addEventListener('keydown', handleEscape);
        } else {
            document.removeEventListener('keydown', handleEscape);
        }
    }

    function handleEscape(event) {
        if (event.key === 'Escape') {
            event.preventDefault();
            setPanelOpen(false);
        }
    }

    function showLoading() {
        loadingState.hidden = false;
        emptyState.hidden = true;
        list.hidden = true;
        list.setAttribute('aria-busy', 'true');
    }

    function showEmpty(message = defaultEmptyMessage, iconClass = defaultEmptyIcon) {
        loadingState.hidden = true;
        emptyState.hidden = false;
        list.hidden = true;
        list.setAttribute('aria-busy', 'false');

        if (emptyMessageEl) {
            emptyMessageEl.textContent = message;
        }

        if (emptyIconEl) {
            emptyIconEl.className = typeof iconClass === 'string' ? iconClass : defaultEmptyIcon;
            emptyIconEl.setAttribute('aria-hidden', 'true');
        }
    }

    function renderNotifications(notifications) {
        loadingState.hidden = true;
        emptyState.hidden = true;
        list.hidden = false;
        list.setAttribute('aria-busy', 'false');
        list.innerHTML = '';

        notifications.forEach((notification) => {
            const level = (notification.level || 'INFO').toUpperCase();
            const iconClass = iconMap[level] || iconMap.INFO;

            const item = document.createElement('li');
            item.className = 'notifications-item';
            item.dataset.level = level;

            const iconWrapper = document.createElement('div');
            iconWrapper.className = 'notifications-item__icon';

            const icon = document.createElement('i');
            icon.className = `bi ${iconClass}`;
            icon.setAttribute('aria-hidden', 'true');
            iconWrapper.appendChild(icon);

            const content = document.createElement('div');
            content.className = 'notifications-item__content';

            const message = document.createElement('p');
            message.className = 'notifications-item__message';
            message.textContent = notification.message || 'Notification';

            const meta = document.createElement('span');
            meta.className = 'notifications-item__meta';
            meta.textContent = `${level.charAt(0)}${level.slice(1).toLowerCase()} • ${formatTimestamp(notification.timestamp)}`;

            content.appendChild(message);
            content.appendChild(meta);

            item.appendChild(iconWrapper);
            item.appendChild(content);

            list.appendChild(item);
        });
    }

    function formatTimestamp(timestamp) {
        if (!timestamp) {
            return 'Unknown time';
        }

        const date = new Date(timestamp);
        if (Number.isNaN(date.getTime())) {
            return timestamp;
        }

        return date.toLocaleString(undefined, {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    }

    async function loadNotifications() {
        showLoading();

        try {
            const response = await fetch('/api/notifications', {
                headers: {
                    'Accept': 'application/json'
                },
                credentials: 'same-origin'
            });

            if (!response.ok) {
                throw new Error(`Request failed with status ${response.status}`);
            }

            const data = await response.json();
            const notifications = Array.isArray(data.notifications) ? data.notifications : [];

            if (!notifications.length) {
                showEmpty();
                return;
            }

            renderNotifications(notifications);
        } catch (error) {
            console.error('Failed to load notifications', error);
            showEmpty('We couldn\'t load your notifications right now.', 'bi bi-wifi-off');
        }
    }

    toggle.addEventListener('click', async () => {
        const willOpen = !panel.classList.contains('is-active');
        setPanelOpen(willOpen);

        if (willOpen) {
            await loadNotifications();
        }
    });

    backdrop.addEventListener('click', () => setPanelOpen(false));

    [closeButton, closeFooterButton].forEach((btn) => {
        if (btn) {
            btn.addEventListener('click', () => setPanelOpen(false));
        }
    });

    if (clearButton) {
        clearButton.addEventListener('click', () => {
            list.innerHTML = '';
            showEmpty('Notifications cleared', 'bi bi-check-circle-fill');
        });
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new TradersPostDashboard();
    initializeNotificationsPanel();
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
