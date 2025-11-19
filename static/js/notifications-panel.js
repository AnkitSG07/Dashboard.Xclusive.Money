(function() {
    function initializeNotificationsPanel() {
        const toggle = document.getElementById('notificationsToggle');
        const drawer = document.getElementById('notificationsDrawer');
        const overlay = document.getElementById('notificationsOverlay');
        const list = document.getElementById('notificationsList');
        const loadingState = document.getElementById('notificationsLoading');
        const emptyState = document.getElementById('notificationsEmpty');
        const emptyMessage = emptyState ? emptyState.querySelector('.empty-state-text') : null;
        const closeButton = document.getElementById('notificationsClose');
        const tabButtons = Array.from(document.querySelectorAll('.tab-button[data-tab]'));
        const headerBadge = document.getElementById('notificationBadge');
        const drawerBadgeContainer = document.getElementById('drawerNotificationBadge');
        const drawerBadge = document.getElementById('drawerUnreadBadge');
        const clearButton = document.getElementById('notificationsClear');

        if (!toggle || !drawer || !overlay || !list || !loadingState || !emptyState) {
            return;
        }

        if (toggle.dataset.notificationsBound === 'true') {
            return;
        }

        let notifications = [];
        let currentTab = 'all';
        let isLoading = false;
        let isClearing = false;
        const warmedCache = window.__WARMED_CACHE__ || {};

        if (clearButton && !clearButton.dataset.defaultLabel) {
            clearButton.dataset.defaultLabel = clearButton.textContent;
        }

        function updateClearButtonState() {
            if (!clearButton) {
                return;
            }
            const hasNotifications = notifications.length > 0;
            const shouldDisable = !hasNotifications || isLoading || isClearing;
            clearButton.disabled = shouldDisable;
            clearButton.setAttribute('aria-disabled', shouldDisable.toString());
        }

        function setDrawerOpen(isOpen) {
            drawer.classList.toggle('active', isOpen);
            overlay.classList.toggle('active', isOpen);
            drawer.setAttribute('aria-hidden', (!isOpen).toString());
            overlay.setAttribute('aria-hidden', (!isOpen).toString());
            toggle.setAttribute('aria-expanded', isOpen.toString());

            if (isOpen) {
                document.addEventListener('keydown', handleEscape);
                loadNotifications();
            } else {
                document.removeEventListener('keydown', handleEscape);
            }
        }

        function handleEscape(event) {
            if (event.key === 'Escape') {
                event.preventDefault();
                setDrawerOpen(false);
            }
        }

        function showLoading() {
            isLoading = true;
            loadingState.hidden = false;
            emptyState.hidden = true;
            list.hidden = true;
            updateClearButtonState();
        }

        function showEmpty(message) {
            isLoading = false;
            loadingState.hidden = true;
            list.hidden = true;
            emptyState.hidden = false;
            if (emptyMessage && typeof message === 'string') {
                emptyMessage.textContent = message;
            }
            updateClearButtonState();
        }

        function updateBadges(count) {
            const safeCount = Number.isFinite(count) ? count : 0;
            if (headerBadge) {
                if (safeCount > 0) {
                    headerBadge.textContent = Math.min(safeCount, 99).toString();
                    headerBadge.style.display = 'flex';
                } else {
                    headerBadge.style.display = 'none';
                }
            }

            if (drawerBadgeContainer && drawerBadge) {
                if (safeCount > 0) {
                    drawerBadge.textContent = safeCount.toString();
                    drawerBadgeContainer.hidden = false;
                } else {
                    drawerBadgeContainer.hidden = true;
                }
            }
        }

        function normalize(rawNotifications) {
            const levelTitleMap = {
                ERROR: 'Error',
                WARNING: 'Warning',
                SUCCESS: 'Success',
                INFO: 'Update'
            };

            return rawNotifications.map((entry, index) => {
                const level = typeof entry.level === 'string' ? entry.level.trim().toUpperCase() : 'INFO';
                const message = typeof entry.message === 'string' && entry.message.trim()
                    ? entry.message.trim()
                    : 'Notification';
                const timestamp = entry.timestamp || null;
                const title = levelTitleMap[level] || (level.charAt(0) + level.slice(1).toLowerCase());
                const category = level === 'ERROR' ? 'errors' : 'orders';

                return {
                    id: entry.id ?? index,
                    level,
                    title,
                    message,
                    timestamp,
                    category,
                    badgeClass: `level-${level.toLowerCase()}`,
                    levelLabel: level
                };
            });
        }

        function formatTime(timestamp) {
            if (!timestamp) {
                return '';
            }

            const date = new Date(timestamp);
            if (Number.isNaN(date.getTime())) {
                return '';
            }

            return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }

        function formatDate(timestamp) {
            if (!timestamp) {
                return '';
            }

            const date = new Date(timestamp);
            if (Number.isNaN(date.getTime())) {
                return '';
            }

            return date.toLocaleDateString();
        }

        function renderNotifications() {
            isLoading = false;
            if (!notifications.length) {
                showEmpty('No notifications yet');
                return;
            }

            const filtered = notifications.filter((item) => {
                if (currentTab === 'orders') {
                    return item.category === 'orders';
                }
                if (currentTab === 'errors') {
                    return item.category === 'errors';
                }
                return true;
            });

            if (!filtered.length) {
                const message = currentTab === 'errors'
                    ? 'No error notifications'
                    : currentTab === 'orders'
                        ? 'No order notifications'
                        : 'No notifications';
                showEmpty(message);
                return;
            }

            loadingState.hidden = true;
            emptyState.hidden = true;
            list.hidden = false;
            list.innerHTML = '';

            filtered.forEach((item) => {
                const card = document.createElement('div');
                card.className = 'notification-card';
                card.dataset.level = item.level;

                const inner = document.createElement('div');
                inner.className = 'notification-inner';

                const badgeColumn = document.createElement('div');
                badgeColumn.className = 'badge-column';

                const badge = document.createElement('span');
                badge.className = `type-badge ${item.badgeClass}`;
                badge.textContent = item.levelLabel;
                badgeColumn.appendChild(badge);

                const content = document.createElement('div');
                content.className = 'notification-content';

                const titleRow = document.createElement('div');
                titleRow.className = 'notification-title-row';

                const title = document.createElement('h4');
                title.className = 'notification-title';
                title.textContent = item.title;

                const time = document.createElement('span');
                time.className = 'notification-time';
                time.textContent = formatTime(item.timestamp);

                titleRow.appendChild(title);
                titleRow.appendChild(time);

                content.appendChild(titleRow);

                if (item.message) {
                    const description = document.createElement('p');
                    description.className = 'notification-description';
                    description.textContent = item.message;
                    content.appendChild(description);
                }

                const footer = document.createElement('div');
                footer.className = 'notification-footer';

                const account = document.createElement('span');
                account.className = 'account-badge';
                account.textContent = item.category === 'errors' ? 'System Alert' : 'System Update';

                const date = document.createElement('span');
                date.className = 'date-text';
                date.textContent = formatDate(item.timestamp);

                footer.appendChild(account);
                if (date.textContent) {
                    footer.appendChild(date);
                }

                content.appendChild(footer);

                inner.appendChild(badgeColumn);
                inner.appendChild(content);
                card.appendChild(inner);
                list.appendChild(card);
            });

            updateClearButtonState();
        }

        async function loadNotifications() {
            showLoading();

            const warmedNotifications = Array.isArray(warmedCache.notifications)
                ? warmedCache.notifications
                : [];
            if (warmedNotifications.length) {
                notifications = normalize(warmedNotifications);
                updateBadges(notifications.length);
                renderNotifications();
                return;
            }

            try {
                const response = await fetch('/api/notifications', {
                    headers: { 'Accept': 'application/json' },
                    credentials: 'same-origin'
                });

                if (!response.ok) {
                    throw new Error(`Request failed with status ${response.status}`);
                }

                const data = await response.json();
                const rawNotifications = Array.isArray(data.notifications) ? data.notifications : [];
                notifications = normalize(rawNotifications);
                updateBadges(notifications.length);
                renderNotifications();
            } catch (error) {
                console.error('Failed to load notifications', error);
                updateBadges(0);
                showEmpty("We couldn't load your notifications right now.");
            }
        }

        async function clearAllNotifications() {
            if (!clearButton || isClearing || !notifications.length) {
                return;
            }

            const originalLabel = clearButton.dataset.defaultLabel || clearButton.textContent;
            isClearing = true;
            updateClearButtonState();
            clearButton.textContent = 'Clearing…';

            try {
                const response = await fetch('/api/notifications/clear', {
                    method: 'POST',
                    headers: { 'Accept': 'application/json' }
                });

                if (!response.ok) {
                    throw new Error(`Request failed with status ${response.status}`);
                }

                await response.json().catch(() => ({}));
                notifications = [];
                updateBadges(0);
                showEmpty("You're all caught up!");
            } catch (error) {
                console.error('Failed to clear notifications', error);
                if (typeof window.showCustomAlert === 'function') {
                    window.showCustomAlert('Failed to clear notifications. Please try again.', 'Action required');
                }
            } finally {
                isClearing = false;
                clearButton.textContent = originalLabel;
                updateClearButtonState();
            }
        }

        function setActiveTab(tab) {
            currentTab = tab;
            tabButtons.forEach((button) => {
                button.classList.toggle('active', button.dataset.tab === tab);
            });
            renderNotifications();
        }

        toggle.addEventListener('click', () => {
            const willOpen = !drawer.classList.contains('active');
            setDrawerOpen(willOpen);
        });

        overlay.addEventListener('click', () => setDrawerOpen(false));

        if (closeButton) {
            closeButton.addEventListener('click', () => setDrawerOpen(false));
        }

        tabButtons.forEach((button) => {
            button.addEventListener('click', () => {
                const tab = button.dataset.tab || 'all';
                setActiveTab(tab);
            });
        });

        if (clearButton && clearButton.dataset.notificationsBound !== 'true') {
            clearButton.addEventListener('click', () => {
                if (!clearButton.disabled) {
                    clearAllNotifications();
                }
            });
            clearButton.dataset.notificationsBound = 'true';
        }
    
        toggle.setAttribute('aria-expanded', 'false');
        toggle.dataset.notificationsBound = 'true';
    }

    window.initializeNotificationsPanel = initializeNotificationsPanel;

    if (document.readyState !== 'loading') {
        initializeNotificationsPanel();
    }

    document.addEventListener('DOMContentLoaded', () => {
        initializeNotificationsPanel();
    });
})();
