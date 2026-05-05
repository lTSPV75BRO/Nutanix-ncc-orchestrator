type Props = {
  page: number;
  pageCount: number;
  onPageChange: (n: number) => void;
};

export function Pagination({ page, pageCount, onPageChange }: Props) {
  if (pageCount <= 1) return null;
  const items = Array.from({ length: pageCount }, (_, idx) => idx + 1).slice(0, 9);
  return (
    <div className="row">
      <button disabled={page <= 1} onClick={() => onPageChange(1)}>
        First
      </button>
      <button disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
        Prev
      </button>
      {items.map((n) => (
        <button key={n} className={n === page ? "btn-active" : ""} onClick={() => onPageChange(n)}>
          {n}
        </button>
      ))}
      <button disabled={page >= pageCount} onClick={() => onPageChange(page + 1)}>
        Next
      </button>
      <button disabled={page >= pageCount} onClick={() => onPageChange(pageCount)}>
        Last
      </button>
    </div>
  );
}
