import Image from "next/image";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { getImage } from "@/lib/placeholder-images";
import { ScrollFadeIn } from "./ScrollFadeIn";

const storyData = [
  {
    date: "April 2021",
    title: "How We Met",
    description: "Our story began at a mutual friend's gathering. A conversation about classic Kannada movies sparked an instant connection, and we spent the rest of the evening lost in conversation.",
    imageId: "story-1",
  },
  {
    date: "August 2022",
    title: "The First Adventure",
    description: "A spontaneous road trip to the hills of Coorg. Sharing filter coffee amidst the misty plantations, we knew this was something special. That's when our adventure truly began.",
    imageId: "story-2",
  },
  {
    date: "March 2024",
    title: "The Proposal",
    description: "Under the stars, with the backdrop of the illuminated Mysore Palace, Aarav got down on one knee. It was a perfect, magical moment that marked the beginning of our forever.",
    imageId: "story-3",
  },
];

export default function StorySection() {
  return (
    <section id="story" className="py-16 md:py-24 bg-background overflow-hidden">
      <ScrollFadeIn>
        <div className="container mx-auto px-4">
          <h2 className="text-4xl md:text-5xl font-headline text-center text-accent mb-12">
            Our Love Story
          </h2>
          <div className="relative wrap overflow-hidden p-10 h-full">
            <div className="absolute border-opacity-20 border-primary/30 h-full border" style={{left: '50%'}}></div>

            {storyData.map((item, index) => {
              const image = getImage(item.imageId);
              const isEven = index % 2 === 0;

              return (
                <div key={index} className={cn(
                  "mb-8 flex justify-between w-full",
                  isEven ? "flex-row-reverse" : ""
                )}>
                  <div className="order-1 w-5/12"></div>
                  <div className="z-20 flex items-center order-1 bg-primary shadow-xl w-8 h-8 rounded-full">
                    <h1 className="mx-auto font-semibold text-lg text-primary-foreground">{index + 1}</h1>
                  </div>
                  <div className="order-1 w-5/12 px-2">
                     <Card className="shadow-lg border-primary/20 transition-all duration-300 hover:scale-105 hover:shadow-2xl hover:shadow-primary/20">
                          <CardHeader>
                            <CardTitle className="font-headline text-2xl text-primary">{item.title}</CardTitle>
                            <CardDescription>{item.date}</CardDescription>
                          </CardHeader>
                          <CardContent>
                            <p className="text-muted-foreground">{item.description}</p>
                          </CardContent>
                        </Card>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </ScrollFadeIn>
    </section>
  );
}
