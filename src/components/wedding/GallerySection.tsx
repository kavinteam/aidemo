import Image from "next/image";
import { Card, CardContent } from "@/components/ui/card";
import {
  Carousel,
  CarouselContent,
  CarouselItem,
  CarouselNext,
  CarouselPrevious,
} from "@/components/ui/carousel";
import { getImage } from "@/lib/placeholder-images";
import { ScrollFadeIn } from "./ScrollFadeIn";

const galleryImageIds = ["gallery-1", "gallery-2", "gallery-3", "gallery-4", "gallery-5"];

export default function GallerySection() {
  const images = galleryImageIds.map(id => getImage(id)).filter(Boolean);

  return (
    <section id="gallery" className="py-16 md:py-24 bg-background overflow-hidden bg-silk-pattern">
      <ScrollFadeIn>
      <div className="container mx-auto px-4">
        <h2 className="text-4xl md:text-5xl font-headline text-center text-accent mb-12">
          Our Memories
        </h2>
        <Carousel
          opts={{
            align: "start",
            loop: true,
          }}
          className="w-full max-w-4xl mx-auto"
        >
          <CarouselContent>
            {images.map((image) => (
              image &&
              <CarouselItem key={image.id} className="md:basis-1/2 lg:basis-1/3">
                <div className="p-1">
                  <Card className="overflow-hidden rounded-lg group shadow-lg">
                    <CardContent className="flex aspect-[3/4] items-center justify-center p-0">
                      <Image
                        src={image.imageUrl}
                        alt={image.description}
                        width={600}
                        height={800}
                        className="w-full h-full object-cover transition-transform duration-500 ease-in-out group-hover:scale-110"
                        data-ai-hint={image.imageHint}
                      />
                    </CardContent>
                  </Card>
                </div>
              </CarouselItem>
            ))}
          </CarouselContent>
          <CarouselPrevious className="hidden md:flex transition-transform hover:scale-110" />
          <CarouselNext className="hidden md:flex transition-transform hover:scale-110" />
        </Carousel>
      </div>
      </ScrollFadeIn>
    </section>
  );
}
