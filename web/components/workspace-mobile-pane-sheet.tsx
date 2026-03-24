"use client";

import { ReactNode } from "react";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";

type WorkspaceMobilePaneSheetProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string | null;
  children: ReactNode;
};

export function WorkspaceMobilePaneSheet({
  open,
  onOpenChange,
  title,
  description,
  children,
}: WorkspaceMobilePaneSheetProps) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="bottom"
        className="h-[88vh] rounded-t-2xl p-0 sm:max-w-none"
      >
        <SheetHeader className="border-b px-4 py-3 text-left">
          <SheetTitle>{title}</SheetTitle>
          <SheetDescription className="truncate">
            {description ?? "Edit the selected configuration."}
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-hidden">{children}</div>
      </SheetContent>
    </Sheet>
  );
}
