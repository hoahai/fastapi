export function HeroBanner() {
  return (
    <section className="relative overflow-hidden rounded-3xl border border-blue-100 bg-white shadow-soft">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_82%_30%,rgba(251,146,60,0.45),transparent_34%),radial-gradient(circle_at_58%_30%,rgba(239,68,68,0.38),transparent_42%),radial-gradient(circle_at_10%_34%,rgba(76,29,149,0.48),transparent_38%),linear-gradient(135deg,#f8fafc_6%,#eef2ff_42%,#ffffff_70%)]" />
      <div className="relative flex min-h-[150px] flex-col justify-center gap-2 px-8 py-8 md:min-h-[200px] md:px-12 md:py-10">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-blue-600/80">
          Media Trading Dashboard
        </p>
        <h1 className="text-4xl font-semibold tracking-tight text-slate-900 md:text-5xl">
          TradSphere
        </h1>
        <p className="max-w-xl text-sm text-slate-700">
          Plan, schedule, and monitor media operations with cleaner workflows.
        </p>
      </div>
    </section>
  );
}
