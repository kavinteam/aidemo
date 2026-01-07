"use server";

import { z } from "zod";
import { revalidatePath } from "next/cache";

const rsvpSchema = z.object({
  name: z.string().min(2, "Name must be at least 2 characters."),
  guests: z.string(),
  events: z.array(z.string()).optional(),
  meal: z.enum(["veg", "non-veg"]),
});

export async function submitRsvp(prevState: any, formData: FormData) {
  const validatedFields = rsvpSchema.safeParse({
    name: formData.get("name"),
    guests: formData.get("guests"),
    events: formData.getAll("events"),
    meal: formData.get("meal"),
  });

  if (!validatedFields.success) {
    return {
      message: "Please check your inputs.",
      errors: validatedFields.error.flatten().fieldErrors,
    };
  }

  // In a real application, you would save this data to a database.
  console.log("RSVP Submitted:", validatedFields.data);

  return { message: "Thank you for your RSVP!", errors: {} };
}


const guestWishSchema = z.object({
    name: z.string().min(2, "Name must be at least 2 characters."),
    message: z.string().min(5, "Message must be at least 5 characters."),
});

// This is a mock database. In a real app, use Firestore, a SQL DB, etc.
const guestWishes: { name: string; message: string; date: Date }[] = [
    { name: "Auntie Priya", message: "So excited for you both! Wishing you a lifetime of happiness.", date: new Date() },
    { name: "Rohan's College Crew", message: "Can't wait to celebrate! Let's party!", date: new Date() },
];

export async function getGuestWishes() {
    return guestWishes.sort((a, b) => b.date.getTime() - a.date.getTime());
}

export async function submitGuestWish(prevState: any, formData: FormData) {
    const validatedFields = guestWishSchema.safeParse({
        name: formData.get("name"),
        message: formData.get("message"),
    });

    if (!validatedFields.success) {
        return {
            message: "Please check your inputs.",
            errors: validatedFields.error.flatten().fieldErrors,
        };
    }

    guestWishes.push({ ...validatedFields.data, date: new Date() });

    console.log("Guest Wish Submitted:", validatedFields.data);
    revalidatePath("/");

    return { message: "Thank you for your lovely wish!", errors: {} };
}
