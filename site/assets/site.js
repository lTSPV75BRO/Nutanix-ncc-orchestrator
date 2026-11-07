// Shared page behaviour: the app's light/dark theme and theme-matched
// screenshots. Dark is the default, as in the app. The choice is stored under
// the app's own key ("app.theme", JSON-encoded) on this origin, so the site and
// the demo at demo/ always show the same theme.
(function () {
  "use strict";
  var KEY = "app.theme";
  var root = document.documentElement;
  var media = window.matchMedia ? window.matchMedia("(prefers-color-scheme: dark)") : null;

  function stored() {
    try { return JSON.parse(localStorage.getItem(KEY) || "null"); } catch (e) { return null; }
  }
  function resolve() {
    var t = stored();
    if (t === "auto") return media && !media.matches ? "light" : "dark";
    return t === "light" ? "light" : "dark";
  }
  function apply() {
    var t = resolve();
    root.setAttribute("data-theme", t);
    Array.prototype.forEach.call(document.querySelectorAll("img[data-light][data-dark]"), function (img) {
      var want = img.getAttribute("data-" + t);
      if (img.getAttribute("src") !== want) img.setAttribute("src", want);
    });
    var btn = document.getElementById("theme-toggle");
    if (btn) {
      var next = t === "dark" ? "light" : "dark";
      btn.setAttribute("aria-label", "Switch to " + next + " theme");
      btn.setAttribute("title", "Switch to " + next + " theme");
      btn.innerHTML = t === "dark"
        ? '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 7a5 5 0 100 10 5 5 0 000-10zm0-5a1 1 0 011 1v1a1 1 0 11-2 0V3a1 1 0 011-1zm0 17a1 1 0 011 1v1a1 1 0 11-2 0v-1a1 1 0 011-1zM4.2 4.2a1 1 0 011.4 0l.7.7a1 1 0 11-1.4 1.4l-.7-.7a1 1 0 010-1.4zm13.5 13.5a1 1 0 011.4 0l.7.7a1 1 0 11-1.4 1.4l-.7-.7a1 1 0 010-1.4zM2 12a1 1 0 011-1h1a1 1 0 110 2H3a1 1 0 01-1-1zm17 0a1 1 0 011-1h1a1 1 0 110 2h-1a1 1 0 01-1-1zM4.9 19.1a1 1 0 010-1.4l.7-.7a1 1 0 111.4 1.4l-.7.7a1 1 0 01-1.4 0zM17.7 6.3a1 1 0 010-1.4l.7-.7a1 1 0 111.4 1.4l-.7.7a1 1 0 01-1.4 0z"/></svg>'
        : '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.4 14.6A8.5 8.5 0 019.4 3.6a.75.75 0 00-.95-.95A10 10 0 1021.35 15.55a.75.75 0 00-.95-.95z"/></svg>';
    }
  }

  root.setAttribute("data-theme", resolve()); // before first paint
  document.addEventListener("DOMContentLoaded", function () {
    apply();
    var btn = document.getElementById("theme-toggle");
    if (btn) btn.addEventListener("click", function () {
      var next = resolve() === "dark" ? "light" : "dark";
      try { localStorage.setItem(KEY, JSON.stringify(next)); } catch (e) {}
      apply();
    });
  });
  if (media && media.addEventListener) media.addEventListener("change", apply);
  window.addEventListener("storage", function (e) { if (e.key === KEY) apply(); });

  // Latest version in the header chip and hero pill (release.js announces it).
  document.addEventListener("ncc-release", function (e) {
    var rel = e.detail;
    if (!rel) return;
    Array.prototype.forEach.call(document.querySelectorAll("[data-latest]"), function (n) { n.textContent = rel.tag_name; n.hidden = false; });
    Array.prototype.forEach.call(document.querySelectorAll("[data-latest-text]"), function (n) { n.textContent = rel.tag_name + " is the latest release"; });
  });
})();
