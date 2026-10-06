(() => {
  const { gsap, ScrollTrigger, MotionPathPlugin } = window;
  if (!gsap || !ScrollTrigger || !MotionPathPlugin) return;

  gsap.registerPlugin(ScrollTrigger, MotionPathPlugin);

  const motion = gsap.matchMedia();
  motion.add("(prefers-reduced-motion: no-preference)", () => {
    const diagram = document.querySelector(".hero-visual");
    const packet = document.querySelector("#signal-packet");
    let cleanupVisibility = () => {};

    if (diagram && packet) {
      const signal = gsap.timeline({ paused: true, repeat: -1, repeatDelay: 1.1 });
      signal
        .set(packet, { autoAlpha: 1, scale: 0.8, transformOrigin: "center" })
        .to(packet, { duration: 1.05, motionPath: { path: "#route-source", align: "#route-source", alignOrigin: [0.5, 0.5] }, ease: "none" })
        .to(".stage-ingest", { scale: 1.025, transformOrigin: "center", duration: 0.18, yoyo: true, repeat: 1 }, "-=0.05")
        .to(packet, { duration: 1.1, motionPath: { path: "#route-context", align: "#route-context", alignOrigin: [0.5, 0.5] }, ease: "none" })
        .to(".stage-context", { scale: 1.018, transformOrigin: "center", duration: 0.18, yoyo: true, repeat: 1 }, "-=0.05")
        .to(packet, { duration: 0.7, motionPath: { path: "#route-rules", align: "#route-rules", alignOrigin: [0.5, 0.5] }, ease: "none" })
        .to(".stage-rules", { scale: 1.018, transformOrigin: "center", duration: 0.18, yoyo: true, repeat: 1 }, "-=0.05")
        .to(packet, { duration: 1.1, motionPath: { path: "#route-finding", align: "#route-finding", alignOrigin: [0.5, 0.5] }, ease: "none" })
        .to(".stage-finding", { scale: 1.018, transformOrigin: "center", duration: 0.18, yoyo: true, repeat: 1 }, "-=0.05")
        .to(packet, { autoAlpha: 0, scale: 1.35, duration: 0.24, ease: "power2.out" });

      ScrollTrigger.create({
        trigger: diagram,
        start: "top bottom",
        end: "bottom top",
        onEnter: () => signal.play(),
        onEnterBack: () => signal.play(),
        onLeave: () => signal.pause(),
        onLeaveBack: () => signal.pause()
      });

      const handleVisibilityChange = () => {
        if (document.hidden) signal.pause();
        else if (ScrollTrigger.isInViewport(diagram)) signal.play();
      };
      document.addEventListener("visibilitychange", handleVisibilityChange);
      cleanupVisibility = () => document.removeEventListener("visibilitychange", handleVisibilityChange);
    }

    const steps = gsap.utils.toArray(".flow-list li");
    if (steps.length) {
      gsap.from(steps, {
        y: 12,
        autoAlpha: 0,
        duration: 0.42,
        stagger: 0.08,
        ease: "power2.out",
        scrollTrigger: { trigger: ".flow-list", start: "top 84%", once: true }
      });
    }

    const statusColumns = gsap.utils.toArray(".status-column");
    if (statusColumns.length) {
      gsap.from(statusColumns, {
        y: 14,
        autoAlpha: 0,
        duration: 0.48,
        stagger: 0.14,
        ease: "power2.out",
        scrollTrigger: { trigger: ".status-columns", start: "top 86%", once: true }
      });
    }
    return cleanupVisibility;
  });
})();
