'use client';

import { Suspense, lazy } from 'react';

const Spline = lazy(() => import('@splinetool/react-spline'));

interface SplineSceneProps {
  scene: string;
  className?: string;
}

export function SplineScene({ scene, className }: SplineSceneProps) {
  return (
    <Suspense
      fallback={
        <div className="spline-loader-wrap">
          <span className="loader" />
        </div>
      }
    >
      <Spline scene={scene} className={className} />
    </Suspense>
  );
}
