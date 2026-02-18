'use client';

interface PDFViewerProps {
  url: string;
}

export default function PDFViewer({ url }: PDFViewerProps) {
  if (!url) {
    return (
      <div className="h-full flex items-center justify-center bg-gray-50">
        <p className="text-gray-500">No PDF URL provided</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full bg-gray-100">
      {/* PDF iframe - simple and reliable */}
      <iframe
        src={url}
        className="w-full h-full border-0"
        title="Certificate PDF"
      />
    </div>
  );
}
