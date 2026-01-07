"use client";

import { useActionState } from "react";
import { useFormStatus } from "react-dom";
import { submitGuestWish, getGuestWishes } from "@/app/actions";
import { useEffect, useState, useRef } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { formatDistanceToNow } from "date-fns";
import { ScrollFadeIn } from "./ScrollFadeIn";

type GuestWish = {
    name: string;
    message: string;
    date: Date;
};

function SubmitButton() {
  const { pending } = useFormStatus();
  return (
    <Button type="submit" disabled={pending} className="w-full md:w-auto bg-primary hover:bg-primary/90 text-primary-foreground">
      {pending ? "Sending..." : "Send Wish"}
    </Button>
  );
}

function WishCard({ wish }: { wish: GuestWish }) {
    const [timeAgo, setTimeAgo] = useState("");

    useEffect(() => {
        setTimeAgo(formatDistanceToNow(wish.date, { addSuffix: true }));
    }, [wish.date]);


    return (
        <Card className="bg-secondary/30">
            <CardHeader className="p-4">
                <div className="flex justify-between items-center">
                <CardTitle className="text-base font-semibold text-accent">{wish.name}</CardTitle>
                <CardDescription className="text-xs">
                    {timeAgo}
                </CardDescription>
                </div>
            </CardHeader>
            <CardContent className="p-4 pt-0">
                <p className="text-muted-foreground">{wish.message}</p>
            </CardContent>
        </Card>
    );
}

export default function GuestWishesSection() {
  const [state, formAction] = useActionState(submitGuestWish, { message: "", errors: {} });
  const [wishes, setWishes] = useState<GuestWish[]>([]);
  const formRef = useRef<HTMLFormElement>(null);

  useEffect(() => {
    async function fetchWishes() {
      const fetchedWishes = await getGuestWishes();
      // Since date objects are not serializable from server actions, we need to convert them back
      const parsedWishes = fetchedWishes.map(w => ({...w, date: new Date(w.date)}));
      setWishes(parsedWishes);
    }
    fetchWishes();
  }, [state]); // Refetch when form state changes (i.e., after submission)

  useEffect(() => {
    if (state.message && !state.errors.name && !state.errors.message) {
      formRef.current?.reset();
    }
  }, [state]);

  return (
    <section id="wishes" className="py-16 md:py-24 bg-background overflow-hidden">
        <ScrollFadeIn>
      <div className="container mx-auto px-4 max-w-4xl">
        <h2 className="text-4xl md:text-5xl font-headline text-center text-accent mb-12">
          Guest Wishes
        </h2>
        <div className="grid md:grid-cols-2 gap-12">
          <div className="order-2 md:order-1">
            <h3 className="font-headline text-2xl text-primary mb-6">Leave a Message</h3>
            <form action={formAction} ref={formRef} className="space-y-4">
              <div>
                <Input name="name" placeholder="Your Name" />
                {state.errors?.name && <p className="text-destructive text-sm mt-1">{state.errors.name[0]}</p>}
              </div>
              <div>
                <Textarea name="message" placeholder="Your wish for the couple..." />
                {state.errors?.message && <p className="text-destructive text-sm mt-1">{state.errors.message[0]}</p>}
              </div>
              <SubmitButton />
              {state.message && !state.errors.name && !state.errors.message && <p className="text-primary mt-2">{state.message}</p>}
            </form>
          </div>
          <div className="order-1 md:order-2 space-y-4 max-h-96 overflow-y-auto pr-2">
            {wishes.map((wish, index) => (
              <WishCard key={index} wish={wish} />
            ))}
          </div>
        </div>
      </div>
      </ScrollFadeIn>
    </section>
  );
}
