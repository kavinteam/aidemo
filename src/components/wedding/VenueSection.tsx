import Image from "next/image";
import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { MapPin } from "lucide-react";
import { getImage } from "@/lib/placeholder-images";
import { ScrollFadeIn } from "./ScrollFadeIn";

const mainVenue = {
  name: "The Tamarind Tree",
  address: "88, Kanakapura Road, Avalahalli, JP Nagar 9th Phase, Bengaluru, Karnataka 560062",
  description: "A place where the old, the new, and the magical artfully blend. The Tamarind Tree is a heritage venue that promises a wedding experience like no other.",
};

export default function VenueSection() {
  const mapImage = getImage("map-placeholder");

  return (
    <section id="venue" className="py-16 md:py-24 bg-background overflow-hidden">
        <ScrollFadeIn>
      <div className="container mx-auto px-4">
        <h2 className="text-4xl md:text-5xl font-headline text-center text-accent mb-12">
          The Wedding Venue
        </h2>
        <div className="grid md:grid-cols-5 gap-8 items-center max-w-6xl mx-auto">
          <div className="md:col-span-2">
            <Card className="border-0 shadow-none">
              <CardHeader>
                <CardTitle className="font-headline text-4xl text-primary">{mainVenue.name}</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <p className="text-muted-foreground">{mainVenue.description}</p>
                <div className="flex items-start gap-3 pt-4">
                  <MapPin className="w-6 h-6 text-accent shrink-0 mt-1" />
                  <div>
                    <p className="font-semibold">{mainVenue.address}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
          <div className="md:col-span-3">
            {mapImage && (
              <Link
                href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(mainVenue.address)}`}
                target="_blank"
                rel="noopener noreferrer"
                className="block rounded-lg overflow-hidden shadow-2xl border-4 border-primary/20 hover:border-primary/50 transition-all duration-300 hover:scale-105"
              >
                <Image
                  src={mapImage.imageUrl}
                  alt="Map to the venue"
                  width={1200}
                  height={800}
                  className="w-full h-full object-cover"
                  data-ai-hint={mapImage.imageHint}
                />
              </Link>
            )}
          </div>
        </div>
      </div>
      </ScrollFadeIn>
    </section>
  );
}
