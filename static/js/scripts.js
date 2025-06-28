/* Description: Custom JS file */

/* --- Navbar Collapse on Scroll --- */
window.onscroll = function () {
  scrollFunction();
  scrollFunctionBTT(); // back to top button
};

window.onload = function () {
  scrollFunction();
};

function scrollFunction() {
  const navbar = document.getElementById("navbarExample");
  if (!navbar) return;
  if (document.documentElement.scrollTop > 30) {
    navbar.classList.add("top-nav-collapse");
  } else if (document.documentElement.scrollTop < 30) {
    navbar.classList.remove("top-nav-collapse");
  }
}

/* --- Hamburger / Offcanvas (Mobile Navbar) --- */
document.addEventListener('DOMContentLoaded', function () {
  const navToggler = document.getElementById('navbarSideCollapse');
  const offcanvas = document.getElementById('navbarsExampleDefault');

  // Hamburger toggle
  if (navToggler && offcanvas) {
    navToggler.addEventListener('click', function (e) {
      e.preventDefault();
      offcanvas.classList.toggle('open');
    });
  }

  // Close menu on nav-link click (except .dropdown-toggle)
  if (offcanvas) {
    const navLinks = offcanvas.querySelectorAll('.nav-link:not(.dropdown-toggle)');
    navLinks.forEach(function(link) {
      link.addEventListener('click', function() {
        offcanvas.classList.remove('open');
      });
    });
  }

  /* --- Dropdown Hover on Desktop, Click on Mobile --- */
  const dropdown = document.querySelector('.dropdown');
  if (dropdown) {
    function toggleDropdown(e) {
      const _d = e.target.closest(".dropdown");
      let _m = _d ? _d.querySelector(".dropdown-menu") : null;
      if (!_d || !_m) return;
      setTimeout(function () {
        const shouldOpen = _d.matches(":hover");
        _m.classList.toggle("show", shouldOpen);
        _d.classList.toggle("show", shouldOpen);
        _d.setAttribute("aria-expanded", shouldOpen);
      }, e.type === "mouseleave" ? 300 : 0);
    }

    dropdown.addEventListener("mouseleave", toggleDropdown);
    dropdown.addEventListener("mouseover", toggleDropdown);
    dropdown.addEventListener("click", (e) => {
      const _d = e.target.closest(".dropdown");
      let _m = _d ? _d.querySelector(".dropdown-menu") : null;
      if (!_d || !_m) return;
      if (_d.classList.contains("show")) {
        _m.classList.remove("show");
        _d.classList.remove("show");
      } else {
        _m.classList.add("show");
        _d.classList.add("show");
      }
    });
  }
});

/* --- Rotating Text - Word Cycle --- */
document.addEventListener('DOMContentLoaded', function () {
  var checkReplace = document.querySelector('.replace-me');
  if (checkReplace !== null && typeof ReplaceMe === "function") {
    var replace = new ReplaceMe(checkReplace, {
      animation: 'animated fadeIn',        // Animation class or classes
      speed: 2000,                         // Delay between each phrase in milliseconds
      separator: ',',                      // Phrases separator
      hoverStop: false,                    // Stop rotator on phrase hover
      clickChange: false,                  // Change phrase on click
      loopCount: 'infinite',               // Loop Count - 'infinite' or number
      autoRun: true,                       // Run rotator automatically
      onInit: false,                       // Function
      onChange: false,                     // Function
      onComplete: false                    // Function
    });
  }
});

/* --- Card Slider - Swiper --- */
document.addEventListener('DOMContentLoaded', function () {
  if (typeof Swiper === "function") {
    var cardSlider = new Swiper('.card-slider', {
      autoplay: {
        delay: 4000,
        disableOnInteraction: false
      },
      loop: true,
      navigation: {
        nextEl: '.swiper-button-next',
        prevEl: '.swiper-button-prev'
      },
      slidesPerView: 3,
      spaceBetween: 70,
      breakpoints: {
        // when window is <= 767px
        767: {
          slidesPerView: 1
        },
        // when window is <= 991px
        991: {
          slidesPerView: 2,
          spaceBetween: 40
        }
      }
    });
  }
});

/* --- Back To Top Button --- */
let myButton = document.getElementById("myBtn");

function scrollFunctionBTT() {
  if (!myButton) return;
  if (document.body.scrollTop > 20 || document.documentElement.scrollTop > 20) {
    myButton.style.display = "block";
  } else {
    myButton.style.display = "none";
  }
}

// When the user clicks on the button, scroll to the top of the document
function topFunction() {
  document.body.scrollTop = 0; // For Safari
  document.documentElement.scrollTop = 0; // For Chrome, Firefox, IE and Opera
}
