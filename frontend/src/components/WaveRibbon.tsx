// Quiet flowing ribbon of fine lines for the sign-in screen. Each line is a
// sine wave; amplitude, phase and offset shift slightly from line to line so
// the set twists like a ribbon. The path is exactly one wavelength longer than
// the view and repeats every wavelength, so sliding it one wavelength to the
// right (CSS, .auth-wave-track) loops seamlessly: the wave flows left to
// right. Lines use currentColor, which .auth-wave sets from the theme accent.
// Motion stops under prefers-reduced-motion (see styles.css).

const W = 1200;
const H = 320;
const LAMBDA = 1200;
const LINES = 16;
const BASE = 190;
const AMP = 70;
const TWIST = 1.1;
const SPREAD = 50;

function linePath(i: number): string {
  const t = i / (LINES - 1);
  const amp = AMP * (0.65 + 0.35 * Math.sin(Math.PI * t));
  const phase = (t - 0.5) * TWIST;
  const offset = (t - 0.5) * SPREAD;
  const k = (2 * Math.PI) / LAMBDA;
  const pts: string[] = [];
  for (let x = 0; x <= W + LAMBDA; x += 12) {
    // Every term has period λ, so the path repeats exactly each wavelength.
    const y = BASE + offset * Math.cos(k * x) + amp * Math.sin(k * x + phase);
    pts.push(`${x},${y.toFixed(1)}`);
  }
  return `M${pts.join("L")}`;
}

const PATHS = Array.from({ length: LINES }, (_, i) => linePath(i));

export function WaveRibbon() {
  return (
    <div className="auth-wave" aria-hidden="true">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" focusable="false">
        <g className="auth-wave-track" style={{ ["--lambda" as string]: `${LAMBDA}px` }}>
          {PATHS.map((d, i) => (
            <path
              key={i}
              d={d}
              fill="none"
              stroke="currentColor"
              strokeWidth={1}
              strokeOpacity={0.18 + 0.5 * Math.sin((Math.PI * i) / (LINES - 1))}
              vectorEffect="non-scaling-stroke"
            />
          ))}
        </g>
      </svg>
    </div>
  );
}
