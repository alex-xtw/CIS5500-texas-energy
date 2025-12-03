"use client";

import { useDateFilter } from "../../contexts/DateFilterContext";

export function DateRangeFilter() {
  const { tempDateRange, setTempDateRange, applyDateRange } = useDateFilter();

  return (
    <div className="flex items-center gap-4 bg-white px-6 py-4 rounded-lg shadow-sm border border-[#d3dbe8]">
      <label className="text-sm font-medium text-[#011F5B]">Date Range:</label>
      <div className="flex items-center gap-2">
        <label className="text-xs text-gray-600">From:</label>
        <input
          type="date"
          value={tempDateRange.startDate}
          onChange={(e) =>
            setTempDateRange({ ...tempDateRange, startDate: e.target.value })
          }
          className="border border-gray-300 rounded px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-[#011F5B] focus:border-transparent"
        />
      </div>
      <div className="flex items-center gap-2">
        <label className="text-xs text-gray-600">To:</label>
        <input
          type="date"
          value={tempDateRange.endDate}
          onChange={(e) =>
            setTempDateRange({ ...tempDateRange, endDate: e.target.value })
          }
          className="border border-gray-300 rounded px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-[#011F5B] focus:border-transparent"
        />
      </div>
      <button
        onClick={applyDateRange}
        className="bg-[#011F5B] text-white px-4 py-1.5 rounded text-sm font-medium hover:bg-[#023E8A] transition-colors focus:outline-none focus:ring-2 focus:ring-[#011F5B] focus:ring-offset-2"
      >
        Update
      </button>
    </div>
  );
}
