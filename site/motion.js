(() => {
  const { gsap, ScrollTrigger, MotionPathPlugin } = window;
  if (!gsap || !ScrollTrigger || !MotionPathPlugin) return;

  gsap.registerPlugin(ScrollTrigger, MotionPathPlugin);

  const motion = gsap.matchMedia();
  motion.add("(prefers-reduced-motion: no-preference)", () => {
    const diagram = document.querySelector(".hero-visual");
    const packet = document.querySelector("#signal-packet");
    let cleanupVisibility = () => {};

    gsap.timeline({ defaults: { ease: "power3.out" } })
      .from(".hero-copy .availability", { y: 8, autoAlpha: 0, duration: 0.24 })
      .from(".hero-copy h1", { y: 20, autoAlpha: 0, duration: 0.54 }, "-=0.12")
      .from(".hero-intro", { y: 10, autoAlpha: 0, duration: 0.38 }, "-=0.34")
      .from(".hero-actions .button", { y: 8, autoAlpha: 0, stagger: 0.06, duration: 0.26 }, "-=0.16")
      .from(".hero-note", { autoAlpha: 0, duration: 0.18 }, "-=0.1")
      .from(".hero-visual svg", { clipPath: "inset(0 100% 0 0)", duration: 0.68, ease: "power3.inOut" }, "-=0.42");

    if (diagram && packet) {
      const signal = gsap.timeline({ paused: true, repeat: -1, repeatDelay: 1.8 });
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
    const flowList = document.querySelector(".flow-list-track");
    if (steps.length && flowList) {
      const railProgress = flowList.querySelector(".flow-rail-progress");
      const railPacket = flowList.querySelector(".flow-rail-packet");
      const scrollRange = {
        trigger: flowList,
        start: "top 64%",
        end: "bottom 64%",
        scrub: 0.45,
        invalidateOnRefresh: true
      };

      gsap.to(railProgress, {
        scaleY: 1,
        ease: "none",
        scrollTrigger: { ...scrollRange }
      });
      gsap.to(railPacket, {
        y: () => flowList.clientHeight - 64,
        ease: "none",
        scrollTrigger: { ...scrollRange }
      });

      steps.forEach((step) => {
        ScrollTrigger.create({
          trigger: step,
          start: "top 54%",
          end: "bottom 54%",
          toggleClass: { targets: step, className: "is-current" }
        });
      });

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
