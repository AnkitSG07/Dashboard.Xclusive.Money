document.addEventListener('DOMContentLoaded', () => {
  const toggle = document.getElementById('menu-toggle');
  if (toggle) {
    toggle.addEventListener('click', e => {
      e.preventDefault();
      document.getElementById('wrapper').classList.toggle('toggled');
    });
  }
});
