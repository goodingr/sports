# HW6b — Project Report
**Name:** Robert Gooding  
**Email:** goodingr@umich.edu  

## Selected Project
I contributed to **OpenRCT2**, an open-source re-implementation of *RollerCoaster Tycoon 2*. It aims to preserve the original game while adding modern features like multiplayer, improved UI, and support for modern operating systems. It is written primarily in C++ and hosted on GitHub.

## Social Good Indication
**No**, this project does not primarily contribute to Social Good as defined by the assignment (e.g., UN Sustainable Development Goals). It is a recreational video game project.

## Project Context
OpenRCT2 exists to keep a classic game playable on modern hardware, as the original 2002 executable has compatibility issues and fixed resolution limits. Beyond preservation, it extends the game with features the original developers couldn't implement, such as cooperative multiplayer, infinite money modes, and removed object limits. It competes with other theme park simulators like *Planet Coaster* or *Parkitect*, but holds a unique niche due to its nostalgia factor and low system requirements. The developers are motivated by a love for the original game and the technical challenge of reverse-engineering and improving a legacy codebase.

## Project Governance
The project uses a standard GitHub workflow. Contributors fork the repository, create feature branches, and submit Pull Requests (PRs) to the `develop` branch.
*   **Communication:** Primary communication happens on GitHub issues/PRs and a dedicated Discord server. The process is relatively informal but structured.
*   **Acceptance Process:** PRs require at least one approving review from a maintainer. Automated Continuous Integration checks run on every push, verifying compilation across platforms (Windows, Linux, macOS, Android) and enforcing code style.
*   **Standards:** The project enforces strict coding standards (checked by `clang-format` and `clang-tidy`). New features often require discussion in Issues before implementation. There is a strong emphasis on backward compatibility with original save files.

## Task Description
I implemented three distinct tasks:

### 1. Fix Minimap Centering and Marker Alignment (#22792)
The minimap viewport rectangle was not centering correctly on the mouse cursor, and the marker was misaligned on elevated terrain due to a "flat earth" assumption in the coordinate mapping. I implemented a fix that uses raycasting to find the actual terrain height under the viewport center. To prevent jitter, I implemented a "hybrid smoothing" approach that averages the Z-height of the terrain, ensuring the marker moves smoothly while staying accurately positioned over mountains.

### 2. Allow Placing Park Entrances Over Paths (#25368)
Previously, players had to manually remove paths before placing a park entrance, which was a friction point. I modified the `ParkEntrancePlaceAction` to automatically remove existing paths if they obstruct the entrance. This involved updating the `MapPlaceParkEntranceClearFunc` to handle the `GAME_COMMAND_FLAG_APPLY` flag correctly, ensuring that paths are only removed when the user actually clicks to build, not during the "ghost" preview.

### 3. Categorize Toilet Income as Shop Sales (#21912)
Income from toilet admissions was incorrectly categorized as "Ride Tickets" in the financial summary. I modified `PeepInteractWithShop` in `Peep.cpp` to check if the facility is a toilet. If so, the expenditure type is set to `ExpenditureType::shopSales`. This ensures financial reports accurately reflect the nature of the income.

## Submitted Artifacts
*   **Task 1 (Minimap):** [PR #25559 - Fix minimap centering and marker alignment](https://github.com/OpenRCT2/OpenRCT2/pull/25559)
    *   Includes code changes to `Map.cpp` implementing the raycasting and smoothing logic.
*   **Task 2 (Entrance):** [PR #25600 - Allow placing park entrances over paths](https://github.com/OpenRCT2/OpenRCT2/pull/25600)
    *   Includes changes to `ParkEntrancePlaceAction.cpp` and `ConstructionClearance.cpp`.
*   **Task 3 (Toilet):** [PR #25609 - Categorize toilet income as shop sales](https://github.com/OpenRCT2/OpenRCT2/pull/25609)
    *   Includes changes to `Peep.cpp`.

## QA Strategy
My QA strategy focused on **manual verification** and **regression testing**, supplemented by the project's automated CI.
*   **Manual Testing:** For the minimap fix, I tested edge cases like maximum zoom levels and extreme terrain heights (using the "Extreme Heights" scenario) to verify alignment. For the entrance fix, I tested placing entrances over various path types and ensured that "ghost" previews didn't destructively remove paths. For the toilet income fix, I created a new park with only a few rides and toilets that cost $0.20 to use. Then, I opened the financial summary and confirmed that Shop Sales were increasing every time a guest used the restroom.
*   **Reproduction Scripts:** For the toilet income fix, I wrote a C++ reproduction script that programmatically simulated a guest entering a toilet and verified the expenditure type. This allowed me to confirm the fix without waiting for random guest behavior in-game.
*   **CI/Static Analysis:** I relied on the project's GitHub Actions to verify that my changes didn't break builds on other platforms and adhered to the strict linting rules.

## QA Evidence
*   **Code Review Feedback:** For the entrance fix, a maintainer pointed out a potential issue with ghost previews removing paths. I verified that my logic using `GAME_COMMAND_FLAG_GHOST` was correct (preventing removal during preview). In PR #25559, a maintainer commented that my implementation created a glitchy appearance in the Minimap Marker when moving the camera over sloped terrain. We discussed tradeoffs between accuracy and smoothness in the different implementations.
*   **CI Results:** All my PRs passed the automated build and style checks (green checkmarks on GitHub).
*   **Reproduction Script Output:** I verified the toilet income fix by running the reproduction script and observing the debug output confirming `ExpenditureType::shopSales` was used.

```cpp
// reproduce_entrance_standalone.cpp snippet
void reproduce_entrance() {
    // ... setup code ...
    if (canBuild.Error == GameActions::Status::Ok) {
        printf("FAILURE: MapCanConstructWithClearAt allowed placement over path!\n");
    } else {
        printf("SUCCESS: MapCanConstructWithClearAt denied placement over path (Error: %d)\n", static_cast<int>(canBuild.Error));
    }
}
```

## Plan Updates
I made significant changes to my plan from HW6a:
*   **Dropped:** "Replay start error message" (#25115) and "Improve Tile Inspector Delete" (#10691).
*   **Added:** "Park Entrances over Paths" (#25368) and "Toilet Income" (#21912).
*   **Justification:** Two of the original issues seemed to have someone actively working on them, so I decided to look for other issues I could work on. I found the "Entrance over Paths" issue to be a high-value quality of life improvement that was more straightforward to implement but still technically interesting. The "Toilet Income" bug allowed me to demonstrate targeted debugging and reproduction scripting. The Minimap task remained as planned.

## Time Log
*   **11/23 (3 hours):** Explore project source code (6-9 PM)
*   **11/24 (3 hours):** Minimap Bug (3-6 PM)
*   **12/2 (4 hours):** Respond to pull request feedback (4-8 PM)
*   **12/3 (4 hours):** New Feature: Build Park Entrance over existing paths (4-8 PM)
*   **12/4 (1 hour):** Respond to pull request feedback (11-12 PM)
*   **12/5 (6 hours):** Change restroom income category from Rides to Shops (10 AM - 4 PM)
*   **12/6 (3 hours):** Work on report (11 AM - 2 PM)
*   **Total:** 24 hours

## Experiences and Recommendations
Contributing to OpenRCT2 was a profound learning experience that highlighted the differences between academic coding assignments and working on a living, legacy software ecosystem.

**The "Correctness" vs. "Feel" Trade-off**
The most surprising challenge I encountered was during the Minimap task (Issue #22792). Coming from a CS background, I initially approached the problem as a purely mathematical one: the viewport marker was misaligned because the projection math assumed a flat earth (Z=0). I implemented a "correct" raycasting solution that calculated the exact intersection of the camera vector with the terrain. While mathematically perfect, the result was jarring—the marker would jump up and down erratically as the camera panned over rugged terrain. A maintainer (`mixiate`) pointed out that this "glitchy" appearance was worse than the original misalignment. This led to a fascinating discussion about User Experience (UX) constraints. I had to pivot to a "hybrid smoothing" solution that was technically "less correct" (it averaged the terrain height) but "felt better" to the user. This taught me that in software engineering, especially game development, the "right" answer is often a compromise between accuracy and usability.

**Navigating Legacy Code and "Ghost" Logic**
OpenRCT2 is a re-implementation of a game from 2002, and it shows. The codebase is massive and full of patterns that felt alien to me. For the Park Entrance task (#25368), I had to interact with the `ConstructionClearance` system. Understanding the difference between "checking for placement" (Ghost preview) and "actually placing" (Apply) was critical. I initially struggled to find where this distinction was made until I traced the `GAME_COMMAND_FLAG_GHOST` and `GAME_COMMAND_FLAG_APPLY` bitmasks. This experience improved my ability to read code I didn't write—a skill I found much harder than writing new code. I learned to rely on "grepping" for flag usage and reading the call stack to understand the context of a function call.

**Strict Quality Assurance and Tooling**
I was impressed by the project's automated tooling. In school, we often just submit code that produces the right output. In OpenRCT2, the CI pipeline ran `clang-tidy` and `clang-format` on every push. At one point, I used a C-style cast `(void)` to suppress a warning about an unused return value. The maintainer (`Gymnasiast`) immediately flagged this in code review, explaining that `[[maybe_unused]]` or a proper check was the modern C++ standard. This strictness was initially frustrating but ultimately educational. It forced me to write code that wasn't just functional but also idiomatic and maintainable. It showed me that in a large open-source project, code quality is the primary defense against technical debt.

**Recommendations for Future Projects**
If I were starting this project again, I would change my approach to **task selection**. I initially picked the "Replay Error" bug because it sounded simple ("just add a print statement"). However, I failed to verify if I could easily reproduce it. It turned out to depend on complex state that was hard to simulate. In contrast, the "Toilet Income" bug was excellent because I could write a standalone C++ script to reproduce it instantly.
*   **What worked:** Choosing tasks that had a clear "Before/After" state (like the visual minimap bug or the financial category bug).
*   **What didn't work:** Relying on "Good First Issue" labels without verifying reproduction steps first.
*   **What I would do instead:** I would spend the first 5 hours solely on creating reproduction scripts for 3-4 potential issues before writing a single line of fix code. This "Test-First" approach would have saved me from the dead-end tasks I initially selected.

## Advice for Future Students
**Robert:** "Don't be afraid to abandon a chosen task if you find yourself stuck on reproduction; switching to a bug you can reliably trigger is often the fastest path to a solution."

## Optional Extra Credit
**Yes.** My Pull Request #25609 ("Categorize toilet income as shop sales instead of ride tickets") was **accepted and merged** into the `develop` branch.
*   **Link:** [https://github.com/OpenRCT2/OpenRCT2/pull/25609](https://github.com/OpenRCT2/OpenRCT2/pull/25609)
*   **Evidence:** The PR status is "Merged" (purple icon), and the changes are now part of the official codebase.
