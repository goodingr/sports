document.addEventListener("DOMContentLoaded", function () {
    const forms = document.querySelector(".forms"),
          pwShowHide = document.querySelectorAll(".eye-icon"),
          links = document.querySelectorAll(".link");

    // Function to toggle password visibility
    function togglePasswordVisibility(eyeIcon) {
        let pwFields = eyeIcon.parentElement.parentElement.querySelectorAll(".password");

        pwFields.forEach(password => {
            if (password.type === "password") {
                password.type = "text";
                eyeIcon.classList.replace("bx-hide", "bx-show");
            } else {
                password.type = "password";
                eyeIcon.classList.replace("bx-show", "bx-hide");
            }
        });
    }

    pwShowHide.forEach(eyeIcon => {
        eyeIcon.addEventListener("click", () => {
            togglePasswordVisibility(eyeIcon);
        });
    });

    // links.forEach(link => {
    //     link.addEventListener("click", e => {
    //         e.preventDefault(); // Prevent form submission
    //         forms.classList.toggle("show-signup");
    //     });
    // });

    // Preloader
    function fadeout() {
        document.querySelector('.preloader').style.opacity = '0';
        setTimeout(() => {
            document.querySelector('.preloader').style.display = 'none';
        }, 500);
    }

    // Call the preloader function when the window loads
    window.onload = function () {
        fadeout();
    };
});