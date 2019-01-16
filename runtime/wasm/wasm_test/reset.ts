import "allocator/arena";

export { memory };

let count: u32 = 0;

export function incr_and_return(): u32 {
    count += 1;
    return count;
}
