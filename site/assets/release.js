// Loads the latest GitHub release of NCC Orchestrator and announces it with a
// "ncc-release" event (detail = the release object, or null if unavailable).
//
// Order: the GitHub REST API from the visitor's browser (always current, 60
// unauthenticated requests per hour per IP), then data/latest-release.json,
// which the Pages workflow writes at deploy time and refreshes whenever a
// release is published. Either way, new releases appear without editing the site.
(function () {
  "use strict";
  var REPO = "lTSPV75BRO/Nutanix-ncc-orchestrator";
  var API = "https://api.github.com/repos/" + REPO + "/releases/latest";
  var SNAPSHOT = (document.currentScript && document.currentScript.dataset.snapshot) || "data/latest-release.json";

  function get(url, headers) {
    return fetch(url, { headers: headers || {} }).then(function (r) {
      if (!r.ok) throw new Error(url + " → HTTP " + r.status);
      return r.json();
    });
  }

  function usable(rel) {
    return rel && typeof rel.tag_name === "string" && Array.isArray(rel.assets) ? rel : null;
  }

  var promise = get(API, { Accept: "application/vnd.github+json" })
    .then(function (rel) {
      rel = usable(rel);
      if (!rel) throw new Error("unexpected API answer");
      rel._source = "live";
      return rel;
    })
    .catch(function () {
      return get(SNAPSHOT).then(function (rel) {
        rel = usable(rel);
        if (rel) rel._source = "snapshot";
        return rel;
      });
    })
    .catch(function () {
      return null;
    });

  window.nccRelease = promise;
  promise.then(function (rel) {
    document.dispatchEvent(new CustomEvent("ncc-release", { detail: rel }));
  });
})();
