import Header from '@/components/wedding/Header';
import HeroSection from '@/components/wedding/HeroSection';
import StorySection from '@/components/wedding/StorySection';
import EventsSection from '@/components/wedding/EventsSection';
import VenueSection from '@/components/wedding/VenueSection';
import GallerySection from '@/components/wedding/GallerySection';
import RsvpSection from '@/components/wedding/RsvpSection';
import GuestWishesSection from '@/components/wedding/GuestWishesSection';
import Footer from '@/components/wedding/Footer';

export default function Home() {
  return (
    <>
      <Header />
      <main className="flex-1">
        <HeroSection />
        <StorySection />
        <EventsSection />
        <VenueSection />
        <GallerySection />
        <RsvpSection />
        <GuestWishesSection />
      </main>
      <Footer />
    </>
  );
}
