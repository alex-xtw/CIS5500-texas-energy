"use client";

import React, { createContext, useContext, useState, ReactNode } from "react";

export interface DateRange {
  startDate: string;
  endDate: string;
}

interface DateFilterContextType {
  dateRange: DateRange;
  tempDateRange: DateRange;
  setTempDateRange: (range: DateRange) => void;
  applyDateRange: () => void;
}

const DateFilterContext = createContext<DateFilterContextType | undefined>(
  undefined
);

export function DateFilterProvider({ children }: { children: ReactNode }) {
  const initialRange: DateRange = {
    startDate: "2010-06-01",
    endDate: "2011-01-01",
  };

  const [dateRange, setDateRange] = useState<DateRange>(initialRange);
  const [tempDateRange, setTempDateRange] = useState<DateRange>(initialRange);

  const applyDateRange = () => {
    setDateRange(tempDateRange);
  };

  return (
    <DateFilterContext.Provider value={{ dateRange, tempDateRange, setTempDateRange, applyDateRange }}>
      {children}
    </DateFilterContext.Provider>
  );
}

export function useDateFilter() {
  const context = useContext(DateFilterContext);
  if (context === undefined) {
    throw new Error("useDateFilter must be used within a DateFilterProvider");
  }
  return context;
}
