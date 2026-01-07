"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";

const WEDDING_DATE = new Date("2024-12-01T10:00:00");

const calculateTimeLeft = () => {
    const difference = +WEDDING_DATE - +new Date();
    if (difference > 0) {
        return {
            days: Math.floor(difference / (1000 * 60 * 60 * 24)),
            hours: Math.floor((difference / (1000 * 60 * 60)) % 24),
            minutes: Math.floor((difference / 1000 / 60) % 60),
            seconds: Math.floor((difference / 1000) % 60),
        };
    }
    return { days: 0, hours: 0, minutes: 0, seconds: 0 };
};

export function Countdown() {
  const [timeLeft, setTimeLeft] = useState({ days: 0, hours: 0, minutes: 0, seconds: 0 });
  const [isClient, setIsClient] = useState(false);

  useEffect(() => {
    // This effect runs only on the client, after the component has mounted.
    setIsClient(true);
    // Set the initial time left immediately on the client
    setTimeLeft(calculateTimeLeft());
    
    const timer = setInterval(() => {
        setTimeLeft(calculateTimeLeft());
    }, 1000);
    
    // Clear interval on component unmount
    return () => clearInterval(timer);
  }, []);

  const timeUnits = [
    { label: "Days", value: isClient ? timeLeft.days : 0 },
    { label: "Hours", value: isClient ? timeLeft.hours : 0 },
    { label: "Minutes", value: isClient ? timeLeft.minutes : 0 },
    { label: "Seconds", value: isClient ? timeLeft.seconds : 0 },
  ];

  return (
    <div className="flex justify-center gap-4 md:gap-8">
      {timeUnits.map((unit, index) => (
        <motion.div 
          key={unit.label} 
          className="flex flex-col items-center"
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: index * 0.1 }}
        >
          <span className="text-4xl md:text-6xl font-headline font-light text-primary">
            {isClient ? String(unit.value).padStart(2, "0") : '--'}
          </span>
          <span className="text-sm md:text-base font-body uppercase tracking-wider text-accent">
            {unit.label}
          </span>
        </motion.div>
      ))}
    </div>
  );
}
