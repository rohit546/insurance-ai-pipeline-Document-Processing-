import { EformsAuthProvider } from '@/components/eforms/AuthProvider';

export default function CstoreLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="min-h-screen" style={{ background: 'white', colorScheme: 'light' }}>
      <EformsAuthProvider>
        {children}
      </EformsAuthProvider>
    </div>
  );
}

