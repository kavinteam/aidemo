"use client";

import { Countdown } from './Countdown';
import { getImage } from '@/lib/placeholder-images';
import { HeroCarousel } from './HeroCarousel';
import { useEffect, useState } from 'react';

const heroImageIds = ['hero', 'hero-2', 'hero-3'];

export default function HeroSection() {
  const images = heroImageIds.map(id => getImage(id)).filter(Boolean);
  const [offsetY, setOffsetY] = useState(0);
  const handleScroll = () => setOffsetY(window.pageYOffset);

  useEffect(() => {
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);


  return (
    <section
      id="home"
      className="relative h-screen min-h-[600px] flex items-center justify-center text-center text-white overflow-hidden"
    >
      <div className="absolute inset-0 w-full h-full" style={{ transform: `translateY(${offsetY * 0.4}px)`}}>
        {images && <HeroCarousel images={images} />}
      </div>

      <div className="absolute inset-0 bg-black/50" />
       <div className="absolute inset-0 bg-gradient-to-t from-background via-transparent to-transparent" />
      <div className="relative z-10 p-4 flex flex-col items-center">
        <h1 className="font-headline text-5xl md:text-8xl text-shadow-lg" style={{textShadow: '0 2px 10px rgba(212,175,55,0.5)'}}>Aarav & Diya</h1>
        <p className="mt-4 text-lg md:text-2xl font-light">
          are getting married!
        </p>
        <p className="mt-2 text-base md:text-xl font-light">
          December 1st, 2024
        </p>
        <div className="my-10 md:my-16 w-full max-w-2xl">
          <Countdown />
        </div>
        <p className="italic text-lg md:text-xl font-body">
          Join us in celebrating our forever
        </p>
      </div>
    </section>
  );
}
