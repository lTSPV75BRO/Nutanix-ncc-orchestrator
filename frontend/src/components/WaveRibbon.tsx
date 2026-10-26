// Flowing ribbon of fine lines for the sign-in screen. Each line is a sine
// wave; amplitude, phase and offset shift slightly from line to line so the
// set twists like a ribbon. A layer's path is exactly one wavelength longer
// than the view and repeats every wavelength, so sliding it one wavelength to
// the right (CSS, .auth-wave-track) loops seamlessly: the wave flows left to
// right. Motion stops under prefers-reduced-motion (see styles.css).

const W = 1200;
const H = 320;

type Layer = { lambda: number; lines: number; base: number; amp: number; twist: number; spread: number };

const FRONT: Layer = { lambda: 1200, lines: 28, base: 185, amp: 78, twist: 1.2, spread: 58 };
const BACK: Layer = { lambda: 1600, lines: 14, base: 165, amp: 60, twist: 0.9, spread: 70 };

function linePath(layer: Layer, i: number): string {
  const t = layer.lines === 1 ? 0.5 : i / (layer.lines - 1);
  const amp = layer.amp * (0.65 + 0.35 * Math.sin(Math.PI * t));
  const phase = (t - 0.5) * layer.twist;
  const offset = (t - 0.5) * layer.spread;
  const k = (2 * Math.PI) / layer.lambda;
  const pts: string[] = [];
  for (let x = 0; x <= W + layer.lambda; x += 12) {
    // Every term has period λ, so the path repeats exactly each wavelength.
    const y = layer.base + offset * Math.cos(k * x) + amp * Math.sin(k * x + phase);
    pts.push(`${x},${y.toFixed(1)}`);
  }
  return `M${pts.join("L")}`;
}

function WaveLayer({ layer, id, className }: { layer: Layer; id: string; className: string }) {
  const paths = Array.from({ length: layer.lines }, (_, i) => linePath(layer, i));
  const core = Math.floor(layer.lines / 2);
  return (
    <g className={className}>
      <defs>
        {/* Colour repeats every wavelength so it travels with the wave without a seam. */}
        <linearGradient id={id} gradientUnits="userSpaceOnUse" x1="0" y1="0" x2={layer.lambda} y2="0" spreadMethod="repeat">
          <stop offset="0" stopColor="#5b21b6" />
          <stop offset="0.45" stopColor="#a855f7" />
          <stop offset="0.6" stopColor="#d8b4fe" />
          <stop offset="1" stopColor="#5b21b6" />
        </linearGradient>
      </defs>
      <g className="auth-wave-track" style={{ ["--lambda" as string]: `${layer.lambda}px` }}>
        {paths.map((d, i) => (
          <path
            key={i}
            d={d}
            fill="none"
            stroke={`url(#${id})`}
            strokeWidth={i === core ? 1.8 : 0.9}
            strokeOpacity={i === core ? 0.95 : 0.28 + 0.4 * Math.sin((Math.PI * i) / (layer.lines - 1))}
          />
        ))}
      </g>
    </g>
  );
}

export function WaveRibbon() {
  return (
    <div className="auth-wave" aria-hidden="true">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" focusable="false">
        <WaveLayer layer={BACK} id="auth-wave-back" className="auth-wave-back" />
        <WaveLayer layer={FRONT} id="auth-wave-front" className="auth-wave-front" />
      </svg>
    </div>
  );
}
