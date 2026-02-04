
import bcrypt from "bcryptjs";

async function main() {
    const password = "Tima11476";
    const salt = await bcrypt.genSalt(10);
    const hash = await bcrypt.hash(password, salt);
    console.log("HASHED_PASSWORD:");
    console.log(hash);
}

main();
