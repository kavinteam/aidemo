"use client";

import { useFormState, useFormStatus } from "react-dom";
import { submitRsvp } from "@/app/actions";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { ScrollFadeIn } from "./ScrollFadeIn";

const events = [
    { id: "mehendi", label: "Mehendi" },
    { id: "sangeet", label: "Sangeet" },
    { id: "wedding", label: "Wedding Ceremony" },
    { id: "reception", label: "Reception" },
];

function SubmitButton() {
    const { pending } = useFormStatus();
    return (
        <Button type="submit" disabled={pending} className="w-full bg-primary hover:bg-primary/90 text-primary-foreground transition-transform hover:scale-105">
            {pending ? "Submitting..." : "Send RSVP"}
        </Button>
    );
}

export default function RsvpSection() {
    const [state, formAction] = useFormState(submitRsvp, { message: "", errors: {} });

    return (
        <section id="rsvp" className="py-16 md:py-24 bg-secondary/30 bg-silk-pattern overflow-hidden">
            <ScrollFadeIn>
            <div className="container mx-auto px-4 max-w-lg">
                <Card className="shadow-2xl border-primary/20 backdrop-blur-sm bg-background/80">
                    <CardHeader className="text-center">
                        <CardTitle className="font-headline text-4xl text-accent">RSVP</CardTitle>
                        <CardDescription className="text-base">We can't wait to celebrate with you!</CardDescription>
                    </CardHeader>
                    <CardContent>
                        {state.message && !state.errors.name ? (
                             <div className="text-center p-8 transition-opacity duration-500">
                                <h3 className="font-headline text-2xl text-primary">{state.message}</h3>
                             </div>
                        ) : (
                        <form action={formAction} className="space-y-6 transition-opacity duration-500">
                            <div className="space-y-2">
                                <Label htmlFor="name">Full Name</Label>
                                <Input id="name" name="name" placeholder="Your full name" />
                                {state.errors?.name && <p className="text-destructive text-sm">{state.errors.name[0]}</p>}
                            </div>
                            
                            <div className="space-y-2">
                                <Label htmlFor="guests">Number of Guests</Label>
                                <Select name="guests" defaultValue="1">
                                    <SelectTrigger id="guests">
                                        <SelectValue placeholder="Select number of guests" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="1">1</SelectItem>
                                        <SelectItem value="2">2</SelectItem>
                                        <SelectItem value="3">3</SelectItem>
                                        <SelectItem value="4">4</SelectItem>
                                    </SelectContent>
                                </Select>
                            </div>
                            
                            <div className="space-y-2">
                                <Label>Events You'll Attend</Label>
                                <div className="space-y-2 rounded-md border p-4">
                                    {events.map((event) => (
                                        <div key={event.id} className="flex items-center space-x-2">
                                            <Checkbox id={`event-${event.id}`} name="events" value={event.id} />
                                            <Label htmlFor={`event-${event.id}`} className="font-normal">{event.label}</Label>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div className="space-y-2">
                                <Label>Meal Preference</Label>
                                <RadioGroup name="meal" defaultValue="veg" className="flex gap-4">
                                    <div className="flex items-center space-x-2">
                                        <RadioGroupItem value="veg" id="veg" />
                                        <Label htmlFor="veg" className="font-normal">Vegetarian</Label>
                                    </div>
                                    <div className="flex items-center space-x-2">
                                        <RadioGroupItem value="non-veg" id="non-veg" />
                                        <Label htmlFor="non-veg" className="font-normal">Non-Vegetarian</Label>
                                    </div>
                                </RadioGroup>
                                 {state.errors?.meal && <p className="text-destructive text-sm">{state.errors.meal[0]}</p>}
                            </div>

                            <SubmitButton />
                        </form>
                        )}
                    </CardContent>
                </Card>
            </div>
            </ScrollFadeIn>
        </section>
    );
}
