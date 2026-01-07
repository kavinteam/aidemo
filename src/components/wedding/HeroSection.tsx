import Image from "next/image";
import { Countdown } from "./Countdown";
import { getImage } from "@/lib/placeholder-images";

export default function HeroSection() {
  const heroImage = getImage("hero");

  return (
    <section id="home" className="relative h-screen min-h-[600px] flex items-center justify-center text-center text-white">
      {heroImage && (
        <Image
          src={heroImage.imageUrl}
          alt={heroImage.description}
          fill
          className="object-cover"
          priority
          data-ai-hint={heroImage.imageHint}
        />
      )}
      <div className="absolute inset-0 bg-black/50" />
      <div className="relative z-10 p-4 flex flex-col items-center">
        <h1 className="font-headline text-5xl md:text-8xl">
          Aarav & Diya
        </h1>
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
