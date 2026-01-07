
"use client";

import { Countdown } from './Countdown';
import { getImage } from '@/lib/placeholder-images';
import { HeroCarousel } from './HeroCarousel';
import { useEffect, useState } from 'react';
import { type ImagePlaceholder } from '@/lib/placeholder-images';

const heroImageIds = ['hero', 'hero-2', 'hero-3', 'hero-4', 'hero-5', 'hero-6', 'hero-7', 'hero-8'];

export default function HeroSection() {
  const [images, setImages] = useState<ImagePlaceholder[]>([]);
  const [scroll, setScroll] = useState(0);
  const [isClient, setIsClient] = useState(false);
  
  useEffect(() => {
    setIsClient(true);
    
    const handleScroll = () => {
      setScroll(window.scrollY);
    };

    window.addEventListener('scroll', handleScroll);
    
    const allImages = heroImageIds.map(id => getImage(id)).filter((img): img is ImagePlaceholder => !!img);
    setImages(allImages);

    return () => {
      window.removeEventListener('scroll', handleScroll);
    };
  }, []);

  const scale = isClient && typeof window !== 'undefined' ? Math.max(0.7, 1 - scroll / window.innerHeight) : 1;

  return (
    <section
      id="home"
      className="h-screen w-full top-0 sticky flex items-center justify-center text-center text-white overflow-hidden"
    >
      {isClient && (
        <div className="absolute inset-0 w-full h-full" style={{ transform: `scale(${scale})`}}>
          {images.length > 0 && <HeroCarousel images={images} />}
          <div className="absolute inset-0 bg-black/50" />
        </div>
      )}

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
