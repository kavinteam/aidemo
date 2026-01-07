"use client";

import { useState, useEffect } from "react";

const WEDDING_DATE = new Date("2024-12-01T10:00:00");

export function Countdown() {
  const [timeLeft, setTimeLeft] = useState({
    days: 0,
    hours: 0,
    minutes: 0,
    seconds: 0,
  });

  useEffect(() => {
    const calculateTimeLeft = () => {
      const difference = +WEDDING_DATE - +new Date();
      if (difference > 0) {
        setTimeLeft({
          days: Math.floor(difference / (1000 * 60 * 60 * 24)),
          hours: Math.floor((difference / (1000 * 60 * 60)) % 24),
          minutes: Math.floor((difference / 1000 / 60) % 60),
          seconds: Math.floor((difference / 1000) % 60),
        });
      } else {
        setTimeLeft({ days: 0, hours: 0, minutes: 0, seconds: 0 });
      }
    };

    const timer = setInterval(calculateTimeLeft, 1000);
    calculateTimeLeft();

    return () => clearInterval(timer);
  }, []);

  const timeUnits = [
    { label: "Days", value: timeLeft.days },
    { label: "Hours", value: timeLeft.hours },
    { label: "Minutes", value: timeLeft.minutes },
    { label: "Seconds", value: timeLeft.seconds },
  ];

  return (
    <div className="flex justify-center gap-4 md:gap-8">
      {timeUnits.map((unit) => (
        <div key={unit.label} className="flex flex-col items-center">
          <span className="text-4xl md:text-6xl font-headline font-light text-primary">
            {String(unit.value).padStart(2, "0")}
          </span>
          <span className="text-sm md:text-base font-body uppercase tracking-wider text-accent">
            {unit.label}
          </span>
        </div>
      ))}
    </div>
  );
}
