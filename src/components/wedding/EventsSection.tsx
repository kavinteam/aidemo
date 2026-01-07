
"use client";

import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { MapPin, Clock, Shirt, Sparkles, Paintbrush, Music2, HeartHandshake, PartyPopper } from "lucide-react";
import Link from "next/link";
import { ScrollFadeIn } from "./ScrollFadeIn";

const events = [
  {
    id: "haldi",
    name: "Haldi Ceremony",
    date: "November 29, 2024",
    time: "10:00 AM",
    venue: "The Bride's Residence",
    address: "123 Saree Street, Bengaluru, Karnataka",
    dressCode: "Shades of Yellow",
    icon: Sparkles,
  },
  {
    id: "mehendi",
    name: "Mehendi Night",
    date: "November 29, 2024",
    time: "6:00 PM",
    venue: "The Bride's Residence",
    address: "123 Saree Street, Bengaluru, Karnataka",
    dressCode: "Vibrant & Festive",
    icon: Paintbrush,
  },
  {
    id: "sangeet",
    name: "Sangeet",
    date: "November 30, 2024",
    time: "7:00 PM",
    venue: "Leela Palace Gardens",
    address: "23, HAL Old Airport Rd, Bengaluru, Karnataka",
    dressCode: "Glamorous Indian Attire",
    icon: Music2,
  },
  {
    id: "wedding",
    name: "Wedding Ceremony",
    date: "December 1, 2024",
    time: "10:00 AM",
    venue: "The Tamarind Tree",
    address: "88, Kanakapura Road, Bengaluru, Karnataka",
    dressCode: "Traditional Indian Wear",
    icon: HeartHandshake,
  },
  {
    id: "reception",
    name: "Reception",
    date: "December 1, 2024",
    time: "7:30 PM",
    venue: "The Tamarind Tree",
    address: "88, Kanakapura Road, Bengaluru, Karnataka",
    dressCode: "Formal / Cocktail Attire",
    icon: PartyPopper,
  },
];

export default function EventsSection() {
  const [activeTab, setActiveTab] = useState("wedding");

  return (
    <section id="events" className="py-16 md:py-24 bg-secondary/30 bg-silk-pattern overflow-hidden">
      <ScrollFadeIn>
      <div className="container mx-auto px-4">
        <h2 className="text-4xl md:text-5xl font-headline text-center text-accent mb-12">
          The Wedding Events
        </h2>
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full max-w-4xl mx-auto">
          <TabsList className="grid w-full grid-cols-2 sm:grid-cols-3 md:grid-cols-5 bg-background/70 backdrop-blur-sm">
            {events.map((event) => (
              <TabsTrigger 
                key={event.id} 
                value={event.id}
                onMouseEnter={() => setActiveTab(event.id)}
              >
                <event.icon className="w-4 h-4 mr-2" />
                {event.name}
              </TabsTrigger>
            ))}
          </TabsList>
          {events.map((event) => (
            <TabsContent key={event.id} value={event.id}>
              <Card className="mt-6 border-primary/20 shadow-lg transition-all duration-300 hover:scale-105 hover:shadow-2xl hover:shadow-primary/20">
                <CardHeader>
                  <CardTitle className="font-headline text-3xl text-primary flex items-center gap-4">
                    <event.icon className="w-8 h-8 text-accent" />
                    {event.name}
                  </CardTitle>
                  <CardDescription className="text-lg">{event.date}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4 text-base">
                  <div className="flex items-center gap-3">
                    <Clock className="w-5 h-5 text-accent" />
                    <span>{event.time}</span>
                  </div>
                  <div className="flex items-start gap-3">
                    <MapPin className="w-5 h-5 text-accent mt-1 shrink-0" />
                    <div>
                      <p className="font-semibold">{event.venue}</p>
                      <p className="text-muted-foreground">{event.address}</p>
                       <Link
                        href={`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(event.address)}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-primary hover:underline"
                      >
                        View on Map
                      </Link>
                    </div>
                  </div>
                  <div className="flex items-center gap-3">
                    <Shirt className="w-5 h-5 text-accent" />
                    <span>Dress Code: {event.dressCode}</span>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          ))}
        </Tabs>
      </div>
      </ScrollFadeIn>
    </section>
  );
}
