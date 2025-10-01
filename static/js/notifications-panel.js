// Notifications panel initialization logic extracted from app.js
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
            showEmpty('We couldn't load your notifications right now.', 'bi bi-wifi-off');
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

window.initializeNotificationsPanel = initializeNotificationsPanel;

document.addEventListener('DOMContentLoaded', () => {
    initializeNotificationsPanel();
});
