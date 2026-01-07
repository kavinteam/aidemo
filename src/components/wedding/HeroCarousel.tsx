"use client";

import Image from 'next/image';
import {
  Carousel,
  CarouselContent,
  CarouselItem,
} from '@/components/ui/carousel';
import Autoplay from 'embla-carousel-autoplay';
import { type ImagePlaceholder } from '@/lib/placeholder-images';

type HeroCarouselProps = {
  images: ImagePlaceholder[];
};

export function HeroCarousel({ images }: HeroCarouselProps) {
  return (
    <Carousel
      className="absolute inset-0 w-full h-full"
      plugins={[Autoplay({ delay: 5000, stopOnInteraction: false })]}
      opts={{ loop: true }}
    >
      <CarouselContent className="h-full">
        {images.map(
          (image, index) =>
            image && (
              <CarouselItem key={image.id} className="h-full">
                <Image
                  src={image.imageUrl}
                  alt={image.description}
                  fill
                  className="object-cover"
                  priority={index === 0}
                  data-ai-hint={image.imageHint}
                />
              </CarouselItem>
            )
        )}
      </CarouselContent>
    </Carousel>
  );
}
