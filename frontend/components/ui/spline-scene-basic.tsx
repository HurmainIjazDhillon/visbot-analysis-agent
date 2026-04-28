'use client';

import { useEffect, useRef, useState } from "react";
import { motion, useMotionValue, useSpring, useTransform } from "framer-motion";

import { Spotlight } from "@/components/ui/spotlight";
import { SparklesText } from "@/components/ui/sparkles-text";
import { SplineScene } from "@/components/ui/splite";

export function SplineSceneBasic() {
  const rootRef = useRef<HTMLDivElement>(null);
  const [showGreeting, setShowGreeting] = useState(false);
  const pointerX = useMotionValue(0.82);
  const pointerY = useMotionValue(0.52);

  const smoothX = useSpring(pointerX, { stiffness: 120, damping: 22, mass: 0.7 });
  const smoothY = useSpring(pointerY, { stiffness: 120, damping: 22, mass: 0.7 });

  const offsetX = useTransform(smoothX, (value) => (value - 0.82) * 26);
  const offsetY = useTransform(smoothY, (value) => (value - 0.52) * 16);
  const rotateY = useTransform(smoothX, (value) => (value - 0.82) * 10);
  const rotateX = useTransform(smoothY, (value) => (0.52 - value) * 8);

  useEffect(() => {
    const node = rootRef.current;
    const hero = node?.closest(".hero-card");

    if (!hero) {
      return;
    }

    const handleMouseMove = (event: Event) => {
      const mouseEvent = event as globalThis.MouseEvent;
      const rect = hero.getBoundingClientRect();
      const relativeX = (mouseEvent.clientX - rect.left) / rect.width;
      const relativeY = (mouseEvent.clientY - rect.top) / rect.height;

      pointerX.set(Math.min(Math.max(relativeX, 0.05), 0.98));
      pointerY.set(Math.min(Math.max(relativeY, 0.08), 0.92));
    };

    const handleMouseLeave = () => {
      pointerX.set(0.82);
      pointerY.set(0.52);
    };

    hero.addEventListener("mousemove", handleMouseMove);
    hero.addEventListener("mouseleave", handleMouseLeave);

    return () => {
      hero.removeEventListener("mousemove", handleMouseMove);
      hero.removeEventListener("mouseleave", handleMouseLeave);
    };
  }, [pointerX, pointerY]);

  useEffect(() => {
    if (!showGreeting) {
      return;
    }
    const timer = window.setTimeout(() => setShowGreeting(false), 3500);
    return () => window.clearTimeout(timer);
  }, [showGreeting]);

  return (
    <div className="spline-panel" aria-label="VisBot 3D scene" ref={rootRef}>
      <Spotlight className="spline-demo-spotlight" fill="white" />
      <motion.div
        className="spline-panel-shell"
        style={{
          x: offsetX,
          y: offsetY,
          rotateX,
          rotateY,
        }}
        onClick={() => setShowGreeting(true)}
      >
        <SplineScene
          scene="https://prod.spline.design/kZDDjO5HuC9GJUM2/scene.splinecode"
          className="spline-scene"
        />
      </motion.div>
      {showGreeting ? (
        <div className="robot-greeting-bubble" aria-live="polite">
          <SparklesText
            text="Hi, how may I help you?"
            className="robot-greeting-text"
            sparklesCount={18}
            colors={{ first: "#7aa8ff", second: "#87d1ff" }}
          />
        </div>
      ) : null}
    </div>
  );
}
