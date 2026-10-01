/* Scale an embed page as one picture to fit its iframe.

   Each page is designed at the size of its Experience Builder widget in the
   builder, given as data-base-w / data-base-h on <body>. On a bigger or
   smaller screen the widget box changes size; rather than letting the content
   reflow (tiles drifting apart, empty bands), the body is laid out at
   viewport ÷ scale and then scaled by a CSS transform.

   scale = the smaller of the width and height ratios, so nothing is cut off.
   The other axis gets extra layout room rather than a blank strip, because the
   body is sized to fill the whole viewport at that scale.

   Load it as the first element in <body>, so it runs before the page script
   draws anything. Charts that size themselves from clientWidth/clientHeight
   are unaffected — those are layout values, measured before the transform. */
(function () {
  const body = document.body;
  const BASE_W = Number(body.dataset.baseW);
  const BASE_H = Number(body.dataset.baseH);
  let scale = 1;

  document.documentElement.style.overflow = 'hidden';
  body.style.transformOrigin = '0 0';
  body.style.overflow = 'hidden';

  function fit() {
    const vw = window.innerWidth, vh = window.innerHeight;
    scale = Math.min(vw / BASE_W, vh / BASE_H);
    body.style.width  = vw / scale + 'px';
    body.style.height = vh / scale + 'px';
    body.style.transform = `scale(${scale})`;
  }

  // Pages position tooltips from mouse coordinates; they divide by this.
  window.fitScale = () => scale;

  fit();
  // Capture phase, so this runs before any resize handler the page registered
  // earlier — those redraw charts and need the new body size first.
  window.addEventListener('resize', fit, true);
})();
